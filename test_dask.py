from typing import Dict, Any
from time import time
from dask.distributed import Client

from .sim_fn import characterize

import pdb

def emulate_flow(params: Dict[str, Any], client):

    dtsa_params = params['dtsa_params']
    top_params = params['top_params']

    dtsa_res, dtsa_done = characterize(dtsa_params, 'TEST', 'dtsa', client)
    top_res, top_done = characterize(top_params, 'TEST', 'top', client)

    done = client.submit(all, [dtsa_done, top_done])
    # all_res = list(dtsa_res.values()) + list(top_res.values())
    # pdb.set_trace()
    # tot_res = dict(dtsa=dtsa_res, top_res=top_res)
    print(done.result())
    pdb.set_trace()


if __name__ == '__main__':

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
    emulate_flow(params, client)
    print(f'tasks finished in {time() - s:.4} seconds')
