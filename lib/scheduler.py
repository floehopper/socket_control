import pytz
from sys import stdout
from time import sleep
from datetime import datetime, timedelta
from threading import Thread
from icalendar import Calendar
from urllib2 import urlopen
import recurring_ical_events

class Scheduler(object):
    """
    A scheduler polls an ical endpoint for events to sync a queue of actions.
    Actions in the queue are performed via the action_callback, marking the
    start and end times of the calendar events.
    Actions are mapped via the action_dict from event names to start and stop
    commands to be passed to the action_callback at the start and end times of
    the event.
    """

    def __init__(self, ical_url, action_callback, action_dict,
                 polling_interval=10, loop_interval=5):
        ScheduledAction.set_action_func(action_callback)

        self.ical_url = ical_url
        self.polling_interval = polling_interval
        self.loop_interval = loop_interval
        self.action_dict = action_dict

        self._action_queue = []
        self._queued_actions = {}

    def start(self):
        self._loop_thread = Thread(target=self._scheduler_loop)
        self._loop_thread.daemon = True
        self._alive = True
        self._loop_thread.start()

    def stop(self):
        self._alive = False

    def _fetch_events(self):
        try:
            req = urlopen(self.ical_url)
            c = Calendar.from_ical(req.read())
            req.close()
            now = datetime.utcnow().replace(tzinfo = pytz.utc)
            events = recurring_ical_events.of(c).at(now.date())
            return events
        except Exception, e:
            t = datetime.utcnow().replace(tzinfo = pytz.utc)
            stdout.write('%s : Failed to load Calendar\n' % t.isoformat())

    def _update_event_queue(self, now):
        events = self._fetch_events()
        if events is None:
            # Failed to load calendar... *shrug*
            return
        now = datetime.utcnow().replace(tzinfo = pytz.utc)
        for e in events:
            uid = e["UID"]
            name = e["SUMMARY"]
            start = e["DTSTART"].dt
            end = e["DTEND"].dt
            if end >= now:
                if uid in self._queued_actions:
                    start_action, end_action = self._queued_actions[uid]
                    if (start_action.scheduledTime < now and
                            start > now):
                        # if active event is rescheduled to the future then end
                        # it immediately and requeue the start action
                        end_action.execute()
                        self._action_queue.append(start_action)
                    if (start_action.scheduledTime != start or
                            end_action.scheduledTime != end):
                        stdout.write('%s : Event rescheduled %s\n' %
                                     (now.isoformat(), uid))
                        start_action.scheduledTime = start
                        end_action.scheduledTime = end
                elif name in self.action_dict:
                    stdout.write('%s : Found event %s\n' %
                                 (now.isoformat(), uid))
                    start_action = ScheduledAction(self.action_dict[name][0],
                                                   start,
                                                   uid)
                    end_action = ScheduledAction(self.action_dict[name][1],
                                                 end,
                                                 uid)
                    self._action_queue.append(start_action)
                    self._action_queue.append(end_action)
                    self._queued_actions[uid] = (start_action, end_action)
        self._action_queue.sort(key=lambda c: c.scheduledTime, reverse=True)

    def _check_queue(self, now):
        while (len(self._action_queue) and
               self._action_queue[-1].scheduledTime <= now):
            next_action = self._action_queue.pop()
            if next_action == self._queued_actions[next_action.uid][1]:
                # if it's the end action
                del self._queued_actions[next_action.uid]
            next_action.execute()

    def _scheduler_loop(self):
        last_poll_time = datetime.utcnow().replace(
            tzinfo=pytz.utc) - timedelta(hours=1)
        while self._alive:
            now = datetime.utcnow().replace(tzinfo=pytz.utc)
            if (now - last_poll_time).seconds >= self.polling_interval:
                self._update_event_queue(now)
            self._check_queue(now)
            sleep(self.loop_interval)


class ScheduledAction(object):
    def __init__(self, action, scheduledTime, uid):
        self.action = action
        self.scheduledTime = scheduledTime
        self.uid = uid
        self.canceled = False

    @classmethod
    def set_action_func(cls, actionFunc):
        cls.actionFunc = actionFunc

    def cancel(self):
        self.canceled = True

    def execute(self):
        if not self.canceled:
            t = datetime.utcnow().replace(tzinfo=pytz.utc)
            stdout.write('%s : Performing action %s for %s\n' %
                         (t.isoformat(), self.action, self.uid))
            self.actionFunc(self.action)
