from threading import Thread, Semaphore
from multiprocessing import Queue
from multiprocessing import cpu_count
from itertools import chain
import sqlite3

# Constants
num_cmt_to_fetch = 5000
num_workers = cpu_count() - 1
num_subreddits = 47172 # 47172
num_max_results = 10

# Shared variables
queue = Queue()
conn = None
subreddit_no = 0
subreddit_cur = None
stop = False

done_sems = []
for i in range(num_workers):
    done_sems.append(Semaphore(0))

db_lock = Semaphore()

class ConsumerThread(Thread):
    def __init__(self):
      Thread.__init__(self)
      self.results = [("", "", -1)] * num_max_results
      global conn, subreddit_cur
      conn = sqlite3.connect("reddit.db", check_same_thread=False)
      subreddit_cur = conn.cursor()
      subreddit_query =  """SELECT id, name FROM subreddits"""
      subreddit_cur.execute(subreddit_query)

    # def runx(self):
    #     cProfile.runctx('self.runx()', globals(), locals(), 'profile-%s.prof' % "Producer")

    def run(self):
        while not stop:
            result = queue.get()
            if result != None:
                min_index = 0
                min_value = self.results[min_index][2]
                for i in range(1, num_max_results):
                    if min_value > self.results[i][2]:
                        min_value = self.results[i][2]
                        min_index = i
                if result[2] > min_value:
                    self.results[min_index] = result
                # for i in range(num_max_results):
                #     if self.results[i][2] < result[2]:
                #         self.results[i] = result
                #         break
            print(result)

class ProducerThread(Thread):

    def __init__(self, threadID):
      Thread.__init__(self)
      self.threadID = threadID

    # def runx(self):
    #     cProfile.runctx('self.runx()', globals(), locals(), 'profile-%s.prof' % self.threadID)

    def run(self):
        symbols_dict = {
            ord('\n'): " ",
            ord('`'): " ",
            ord('~'): " ",
            ord('!'): " ",
            ord('@'): " ",
            ord('#'): " ",
            ord('$'): " ",
            ord('%'): " ",
            ord('^'): " ",
            ord('&'): " ",
            ord('*'): " ",
            ord('('): " ",
            ord(')'): " ",
            ord('_'): " ",
            ord('-'): " ",
            ord('+'): " ",
            ord('='): " ",
            ord('{'): " ",
            ord('['): " ",
            ord(']'): " ",
            ord('}'): " ",
            ord('|'): " ",
            ord('\\'): " ",
            ord(':'): " ",
            ord(';'): " ",
            ord('"'): " ",
            ord("'"): " ",
            ord('<'): " ",
            ord('>'): " ",
            ord('.'): " ",
            ord('?'): " ",
            ord('/'): " ",
            ord(','): " "
        }
        global queue, db_lock, conn, subreddit_no
        cursor = conn.cursor()
        while subreddit_no < num_subreddits:
            total_vocabulary = set()
            db_lock.acquire()
            subreddit_id, subreddit_name = subreddit_cur.fetchone()
            subreddit_query = "SELECT body FROM comments WHERE subreddit_id='" + str(subreddit_id) + "'"
            print("Thread", self.threadID, " got subreddit ", subreddit_id, subreddit_name)
            subreddit_no += 1
            cursor.execute(subreddit_query)
            comments = cursor.fetchall()
            db_lock.release()
            while comments:
                cmts = " ".join(chain.from_iterable(comments)).lower()
                cmts = cmts.translate(symbols_dict)
                words = set(cmts.split())
                words.discard('')
                total_vocabulary = total_vocabulary.union(words)

                db_lock.acquire() # Lock for file read
                comments = cursor.fetchmany(num_cmt_to_fetch)
                db_lock.release()
            # Once the subreddit is done
            queue.put((subreddit_id, subreddit_name, len(total_vocabulary))) # Submit the results
        done_sems[self.threadID].release()



consumerThread = ConsumerThread()

consumerThread.start()

threads = []
for i in range(num_workers):
    threads.append(ProducerThread(i))

for thread in threads:
    thread.start()


for thread in threads:
    thread.join()

print("I'm after thread joins")

for i in range(num_workers):
    done_sems[i].acquire()

stop = True
queue.put(None)

print("Results:\n")
consumerThread.results.sort(key=lambda res: res[2])
for i in range(num_max_results):
    print(consumerThread.results[i])

consumerThread.join()

print("I'm after consumer join")
