import json
import socket
import sys

from checks import AgentCheck
from util import headers


class UWSGICheck(AgentCheck):
    """Extracts stats from uWSGI via the uWsgi stats API
    http://uwsgi-docs.readthedocs.io/en/latest/StatsServer.html
    """

    SERVICE_CHECK_NAME = 'uwsgi.can_connect'
    SOURCE_TYPE_NAME = 'uwsgi'
    TIMEOUT = 5

    def __init__(self, name, init_config, agentConfig, instances=None):
        super(UWSGICheck, self).__init__(name, init_config, agentConfig, instances)
        self.last_timestamps = {}

    def _validate_instance(self, instance):
        for key in ['uwsgi_url', 'uwsgi_port']:
            if not key in instance:
                raise Exception("A {} must be specified".format(key))

    def _get_response_from_url(self, url, instance, params=None):
        self.log.debug('Fetching uwsgi stats at url: %s' % url)


        request_headers = headers(self.agentConfig)
        response = requests.get(url, params=params, headers=request_headers,
                                timeout=int(instance.get('timeout', self.TIMEOUT)))
        response.raise_for_status()
        return response

    def _get_data_from_url(self, url, instance):
        "Hit a given URL and return the parsed json"
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(2)
        s.connect((instance['uwsgi_url'], instance['uwsgi_port']))
        data = s.makefile().read(-1)
        s.close()
        return json.loads(data)


    def _safe_get_data_from_url(self, url, instance):
        try:
            data = self._get_data_from_url(url, instance)
        except Exception as e:
            self.warning('Error reading data from URL: {}'.format(url))
            return

        if data is None:
            self.warning("No stats could be retrieved from {}".format(url))

        return data

    def check(self, instance):
        tags = instance.get('tags', [])
        self._validate_instance(instance)
        self.check_connection(instance, tags)
        self.get_uwsgi_stats(instance, tags)

    def check_connection(self, instance, tags):
        url = instance['uwsgi_url'] + ':' + str(instance['uwsgi_port'])
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(2)
        try:
            s.connect((instance['uwsgi_url'], instance['uwsgi_port']))
            s.close()
        except Exception as e:
            self.service_check(self.SERVICE_CHECK_NAME, AgentCheck.CRITICAL, tags=tags, message=str(e))
        else:
            self.service_check(self.SERVICE_CHECK_NAME, AgentCheck.OK, tags=tags,
                               message='Connection to {} was successful'.format(url))

    def create_top_level_stats(self, instance, data, tags):
        # load
        self.gauge(
            '{}.load'.format(self.SOURCE_TYPE_NAME),
            data['load'],
            tags=tags
        )

        # listen queue
        self.gauge(
            '{}.listen_queue'.format(self.SOURCE_TYPE_NAME),
            data['listen_queue'],
            tags=tags
        )

        # listen_queue errors
        self.gauge(
            '{}.listen_queue_errors'.format(self.SOURCE_TYPE_NAME),
            data['listen_queue_errors'],
            tags=tags
        )

        # signal queue
        self.gauge(
            '{}.signal_queue'.format(self.SOURCE_TYPE_NAME),
            data['signal_queue'],
            tags=tags
        )

    def create_worker_stats(self, instance, data, tags):
        for worker_data in data:
            worker_id = worker_data['id']
            worker_tag = 'uwsgi_worker:{}'.format(worker_id)

            # respawn_count
            self.gauge(
                '{}.respawn_count'.format(self.SOURCE_TYPE_NAME),
                worker_data['respawn_count'],
                tags=tags + [worker_tag]
            )

            # tx (transmitted data)
            self.gauge(
                '{}.tx'.format(self.SOURCE_TYPE_NAME),
                worker_data['tx'],
                tags=tags + [worker_tag]
            )

            # vsz (address space)
            self.gauge(
                '{}.vsz'.format(self.SOURCE_TYPE_NAME),
                worker_data['vsz'],
                tags=tags + [worker_tag]
            )

            # harakiri count
            self.gauge(
                '{}.harakiri_count'.format(self.SOURCE_TYPE_NAME),
                worker_data['harakiri_count'],
                tags=tags + [worker_tag]
            )

            # signals (# of managed uwsgi signals NOT unix signals!!!)
            self.gauge(
                '{}.signals'.format(self.SOURCE_TYPE_NAME),
                worker_data['signals'],
                tags=tags + [worker_tag]
            )

            # signal_queue
            self.gauge(
                '{}.signal_queue'.format(self.SOURCE_TYPE_NAME),
                worker_data['signal_queue'],
                tags=tags + [worker_tag]
            )

            # aggregate and get our items we are interested in within the cores objects
            cores_data = {}
            cores_items = ['in_request', 'static_requests', 'routed_requests', 'offloaded_requests', 'read_errors',
                           'write_errors', 'requests']
            for cores_item in cores_items:
                cores_data[cores_item] = 0
                for core in worker_data['cores']:
                    cores_data[cores_item] += core[cores_item]

                self.gauge(
                    '{}.{}'.format(self.SOURCE_TYPE_NAME, cores_item),
                    cores_data[cores_item],
                    tags=tags + [worker_tag]
                )

            # avg_rt (average response time)
            self.gauge(
                '{}.avg_rt'.format(self.SOURCE_TYPE_NAME),
                worker_data['avg_rt'],
                tags=tags + [worker_tag]
            )

            # exceptions
            self.gauge(
                '{}.exceptions'.format(self.SOURCE_TYPE_NAME),
                worker_data['exceptions'],
                tags=tags + [worker_tag]
            )

            # accepting
            self.gauge(
                '{}.accepting'.format(self.SOURCE_TYPE_NAME),
                worker_data['accepting'],
                tags=tags + [worker_tag]
            )

            # requests
            self.gauge(
                '{}.requests'.format(self.SOURCE_TYPE_NAME),
                worker_data['requests'],
                tags=tags + [worker_tag]
            )

            # requests
            self.gauge(
                '{}.running_time'.format(self.SOURCE_TYPE_NAME),
                worker_data['running_time'],
                tags=tags + [worker_tag]
            )

            # rss (rss memory)
            self.gauge(
                '{}.rss'.format(self.SOURCE_TYPE_NAME),
                worker_data['rss'],
                tags=tags + [worker_tag]
            )

    def get_uwsgi_stats(self, instance, tags):
        url = instance['uwsgi_url']
        data = self._safe_get_data_from_url(url, instance)
        worker_data = data.pop('workers', [])
        self.create_top_level_stats(instance, data, tags)
        self.create_worker_stats(instance, worker_data, tags)


if __name__ == '__main__':
    if len(sys.argv) == 2:
        path = sys.argv[1]
    else:
        print "Usage: python uwsgi.py <path_to_config>"
    check, instances = UWSGICheck.from_yaml(path)
    for instance in instances:
        print "\nRunning the check against url: %s" % (instance['uwsgi_url'] + ':' + str(instance['uwsgi_port']))
        check.check(instance)
        if check.has_events():
            print 'Events: %s' % (check.get_events())
        print 'Metrics: %s' % (check.get_metrics())
