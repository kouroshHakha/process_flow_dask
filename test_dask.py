from typing import Dict, Any, List
from time import time
from dask.distributed import Client, get_client, wait
from copy import deepcopy

from sim_fn import characterize, generate_cell

import pdb

def emulate_flow(params: Dict[str, Any]):
    dtsa_params = params['dtsa_params']
    top_params = params['top_params']

    dtsa_done = characterize(dtsa_params, 'TEST', 'dtsa')
    # top_done = characterize(top_params, 'TEST', 'top')
    # done = client.submit(all, [dtsa_done, top_done])
    return dtsa_done
    # return dtsa_done

def run_sim(list_of_params: List[Dict[str, Any]]):
    result_futures = []
    for params in list_of_params:
        result_futures.append(emulate_flow(params))

    wait(result_futures)

    results = []
    for future in result_futures:
        try:
            res = future.result()
        except:
            res = future.exception()
        results.append(res)


if __name__ == '__main__':

    # client = Client(processes=False)
    client = Client(processes=False)
    params = dict(
        dtsa_params=dict(
            layout_params=dict(
                nf=2,
            ),
            wrapper_params=[dict(name='od'), dict(name='noise')],
            sim_params=dict(
                od=dict(type='od'),
                noise=dict(type='noise'),
            )
        ),
        top_params=dict(
            layout_params=dict(
                nf=2,
            ),
            wrapper_params=[dict(name='diff'), dict(name='cm')],
            sim_params=dict(
                diff=dict(type='diff'),
                cm=dict(type='cm')
            )
        ),
    )

    s = time()

    num_jobs = 1
    list_of_params = []
    for i in range(num_jobs):
        new_params = deepcopy(params)
        new_params['dtsa_params']['layout_params']['nf'] = i
        new_params['top_params']['layout_params']['nf'] = i // 2
        list_of_params.append(new_params)
    run_sim(list_of_params)
    print(f'tasks finished in {time() - s:.4} seconds')
