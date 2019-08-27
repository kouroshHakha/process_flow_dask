import dask
from time import sleep, time

from dask.distributed import Client

def task(x, cond):
    if not cond:
        return False
    sleep(x)
    return True

def run_graph(client):
    task1_done = client.submit(task, 2, True)
    task2_done = client.submit(task, 1, True)
    task3_done = client.submit(task, 1, task2_done)

    # task1_done = task1_done.result()
    # task3_done = task3_done.result()
    all_done = task1_done and task3_done
    return all_done

if __name__ == '__main__':
    client = Client()
    s = time()
    done = run_graph(client)
    print(done.result())
    print(f'tasks finished in {time() - s:.4} seconds')
