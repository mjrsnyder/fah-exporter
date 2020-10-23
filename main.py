from prometheus_client.core import GaugeMetricFamily, StateSetMetricFamily, REGISTRY
from prometheus_client import start_http_server
from urllib.parse import urlparse
import asyncio
import time
import os

class FAHCollector(object):
    def __init__(self, url):
        self.url = urlparse(url)
        if not self.url.port:
            self.url.port = 36330

        self.slot_info = None
        self.queue_info = None
        self.ppd = None
        self.up = False

    def get_up_status(self):
        u = GaugeMetricFamily('fah_up', 'FAH Reachability', labels=['instance'])
        u.add_metric([self.url.netloc], int(self.up))
        yield u

    def get_total_ppd(self):
        p = GaugeMetricFamily('fah_ppd', 'Estimated Points Per Day', labels=['instance'])
        p.add_metric([self.url.netloc], float(self.ppd))
        yield p

    def get_queue_state(self):
        q = StateSetMetricFamily('fah_queue_state', 'State of the queue', labels=['id', 'slot', 'core', 'unit'])
        states = {
            'UNKNOWN' : False,
            'PAUSED' : False,
            'UPDATING' : False,
            'FINISHED' : False,
            'UPLOADED' : False,
            'RUNNING' : False,
            'ACTIVE' : False,
            'ONLINE' : False,
            'FINISHING' : False,
            'FAILED' : False,
            'ERROR' : False,
            'FAULTY' : False,
            'SHUTDOWN' : False,
            'CONNECTING' : False,
            'OFFLINE' : False,
            'READY' : False,
            'DUMP' : False,
            'DOWNLOAD' : False
        }
        for queue in self.queue_info:
            queue_states = states
            queue_states[queue['state']] = True
            q.add_metric([queue['id'], queue['slot'], queue['core'], queue['unit']], queue_states)
        yield q

    def get_queue_ppd(self):
        q = GaugeMetricFamily('fah_queue_ppd', 'Estimated points per day of the queue', labels=['id', 'slot', 'core', 'unit'])
        for queue in self.queue_info:
            q.add_metric([queue['id'], queue['slot'], queue['core'], queue['unit']], queue['ppd'])
        yield q

    def get_queue_frames_total(self):
        q = GaugeMetricFamily('fah_queue_frames_total', 'Total number of frames in the job', labels=['id', 'slot', 'core', 'unit'])
        for queue in self.queue_info:
            q.add_metric([queue['id'], queue['slot'], queue['core'], queue['unit']], queue['totalframes'])
        yield q

    def get_queue_frames_complete(self):
        q = GaugeMetricFamily('fah_queue_frames_complete', 'Frames completed in the job', labels=['id', 'slot', 'core', 'unit'])
        for queue in self.queue_info:
            q.add_metric([queue['id'], queue['slot'], queue['core'], queue['unit']], queue['framesdone'])
        yield q

    def get_slot_idle(self):
        s = GaugeMetricFamily('fah_slot_idle', 'Idle state of the slot', labels=['id', 'description'])
        for slot in self.slot_info:
            if slot['idle'] == True:
                slot_idle = 1.0
            else:
                slot_idle = 0.0
            s.add_metric([slot['id'], slot['description']], slot_idle)
        yield s

    def parse(self,data):
        start = data.find('\nPyON ')
        if start != -1:
            eol = data.find('\n', start + 1)
            if eol != -1:
                line = data[start + 1: eol]
                tokens = line.split(None, 2)

                if len(tokens) < 3:
                    data = data[eol:]
                    raise Exception('Invalid PyON line: ' +
                                    line.encode('unicode_escape'))

                end = data.find('\n---\n', start)
                if end != -1:
                    try:
                        msg = eval(data[eol + 1: end], {}, {})
                    except Exception as e:
                        print('ERROR parsing PyON message: %s: %s' % (str(e), data.encode('unicode_escape')))
                    return msg

    async def collect_metrics(self, host, port):
        reader, writer = await self.connect_api(host, port)

        if self.up:
            self.slot_info = await self.run_command(reader, writer, 'slot-info')
            self.queue_info = await self.run_command(reader, writer, 'queue-info')
            self.ppd = await self.run_command(reader, writer, 'ppd')
            writer.close()


    async def connect_api(self, host, port):
        try:
            reader, writer = await asyncio.open_connection(
                host, port)

            data = await reader.read(1024)
            if data:
                print(f'Received: {data.decode()!r}')
                self.up = True
                return reader, writer
            else:
                writer.close()
        except Exception as e:
            print('ERROR: %s' % e )
            return None, None

    async def run_command(self, reader, writer, cmd):
        writer.write(('%s\n' % cmd).encode('ascii'))

        data = await reader.read(2048)
        if data:
            return self.parse(data.decode())

    def collect(self):
        asyncio.run(self.collect_metrics(self.url.hostname, self.url.port))

        for i in self.get_up_status(): yield i

        if self.up:
            # Total Estimated Points Per Day
            for i in self.get_total_ppd(): yield i

            # Queue Metrics
            for i in self.get_queue_state(): yield i
            for i in self.get_queue_ppd(): yield i
            for i in self.get_queue_frames_complete(): yield i
            for i in self.get_queue_frames_total(): yield i

            # Grab some slot stuff too (mostly for the description)
            for i in self.get_slot_idle(): yield i

if __name__ == '__main__':

    # Get config options via env vars
    listen_port = os.environ.get('FAH_EXPORTER_PORT', 9361)

    target_host = os.environ.get('FAH_EXPORTER_HOST', 'tcp://localhost:36330')

    interval = os.environ.get('FAH_EXPORTER_INTERVAL', 5)

    start_http_server(listen_port)
    
    REGISTRY.register(FAHCollector(target_host))

    while True: time.sleep(interval)