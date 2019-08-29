from typing import Dict, Any
from time import sleep
from dask.distributed import get_client

def generate_layout(params: Dict[str, Any], lib_name: str, impl_name: str,
                    dep_list=None):
    print(f'generating layout {lib_name}__{impl_name}_{params["nf"]} ...')
    if params['nf'] == 3:
        raise ValueError('cannot handle nf = 3')
    sleep(1)
    print(f'layout {lib_name}__{impl_name}_{params["nf"]} done.')
    return params

def generate_schematic(params: Dict[str, Any], lib_name: str, impl_name: str,
                       dep_list=None):
    print(f'generating schematic {lib_name}__{impl_name} ...')
    sleep(1)
    print(f'schematic {lib_name}__{impl_name} done.')
    return True

def run_lvs(lib_name: str, impl_name: str, dep_list=None):
    print(f'running lvs on {lib_name}__{impl_name}...')
    sleep(10)
    print(f'lvs on {lib_name}__{impl_name} done.')
    return True

def run_drc(lib_name: str, impl_name: str, dep_list=None):
    print(f'running drc on {lib_name}__{impl_name} ...')
    sleep(10)
    print(f'drc on {lib_name}__{impl_name} done.')
    return True

def run_simulation(params: Dict[str, Any], lib_name: str, impl_name: str, dep_list=None):
    sim_type = params['type']
    print(f'running simulation {sim_type} on {lib_name}__{impl_name}...')

    if sim_type == 'od':
        sleep(1)
    elif sim_type == 'noise':
        sleep(1.5)
    elif sim_type == 'diff':
        sleep(3)
    elif sim_type == 'cm':
        sleep(1)

    print(f'simulation {sim_type} on {lib_name}__{impl_name} done ...')
    return True

def generate_cell(params: Dict[str, Any], lib_name: str, impl_name: str, dep_list=None):
    client = get_client()
    sch_params = client.submit(generate_layout, params['layout_params'], lib_name, impl_name)
    sch_done = client.submit(generate_schematic, sch_params, lib_name, impl_name)
    return sch_done

def create_designs(params, lib_name, impl_name, dep_list=None):
    client = get_client()
    sch_params = client.submit(generate_layout, params['layout_params'], lib_name, impl_name)
    drc_done = client.submit(run_drc, lib_name, impl_name, dep_list=sch_params)

    sch_done = {}
    sch_done['main'] = client.submit(generate_schematic, sch_params, lib_name, impl_name,
                                     dep_list=sch_params)

    lvs_done = client.submit(run_lvs, lib_name, impl_name, dep_list=sch_done['main'])
    for wrapper in params['wrapper_params']:
        sch_done[wrapper['name']] = client.submit(generate_schematic, wrapper, lib_name,
                                                  wrapper['name'], dep_list=sch_done['main'])

    schs_done = client.submit(all, list(sch_done.values()))
    creation_done = client.submit(all, [schs_done, drc_done, lvs_done])
    return creation_done

def verify_designs(params, lib_name, impl_name, ready=False):
    results = {}
    for tb_name, tb_params in params['sim_params'].items():
        tb_cell_name = f'tb_{tb_name}'
        sch_done = client.submit(generate_schematic, tb_params, lib_name, tb_cell_name, ready=ready)
        results[tb_name] = client.submit(run_simulation, tb_params, lib_name, tb_cell_name,
                                         ready=sch_done)
    all_done = client.submit(all, [list(results.values())])
    return results, all_done

def characterize(params: Dict[str, Any], lib_name: str, impl_name: str):
    client = get_client()
    creation_done = create_designs(params, lib_name, impl_name)
    # results, all_done = verify_designs(params, lib_name, impl_name, client, ready=creation_done)
    # return results, all_done
    return creation_done