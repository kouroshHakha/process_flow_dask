from typing import Dict, Any
from time import sleep

def generate_layout(params: Dict[str, Any], lib_name: str, impl_name: str,
                    ready=False):
    print(f'generating layout {lib_name}__{impl_name} ...')
    if not ready: return False
    sleep(5)
    print(f'layout {lib_name}__{impl_name} done.')
    return True

def generate_schematic(params: Dict[str, Any], lib_name: str, impl_name: str,
                       ready=False):
    print(f'generating schematic {lib_name}__{impl_name}, {ready}...')
    if not ready: return False
    sleep(2)
    print(f'schematic {lib_name}__{impl_name} done.')
    return True


def run_lvs(lib_name: str, impl_name: str, ready=False):
    if not ready: return False
    print(f'running lvs on {lib_name}__{impl_name}...')
    sleep(3)
    print(f'lvs on {lib_name}__{impl_name} done.')
    return True

def run_drc(lib_name: str, impl_name: str, ready=False):
    if not ready: return False
    print(f'running drc on {lib_name}__{impl_name} ...')
    sleep(5)
    print(f'drc on {lib_name}__{impl_name} done.')
    return True

def run_simulation(params: Dict[str, Any], lib_name: str, impl_name: str, ready=False):
    sim_type = params['type']
    print(f'running simulation {sim_type} on {lib_name}__{impl_name}...')
    if not ready: return False

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

def generate_cell(params: Dict[str, Any], lib_name: str, impl_name: str, client):
    lay_done = client.submit(generate_layout, params, lib_name, impl_name, ready=True)
    sch_done = client.submit(generate_schematic, params, lib_name, impl_name, ready=lay_done)
    cell_done = lay_done and sch_done
    return cell_done

def create_designs(params, lib_name, impl_name, client):
    lay_done = client.submit(generate_layout, params['layout_params'], lib_name, impl_name, True)
    drc_done = client.submit(run_drc, lib_name, impl_name, ready=lay_done)

    sch_done = {}
    sch_done['main'] = client.submit(generate_schematic, params, lib_name, impl_name, ready=lay_done)

    creation_done = client.submit(all, [sch_done['main'], lay_done])
    lvs_done = client.submit(run_lvs, lib_name, impl_name, ready=creation_done)
    for wrapper in params['wrapper_params']:
        sch_done[wrapper['name']] = client.submit(generate_schematic, wrapper, lib_name,
                                                  wrapper['name'], ready=creation_done)

    schs_done = client.submit(all, list(sch_done.values()))
    creation_done = client.submit(all, [schs_done, drc_done, lvs_done])

    return creation_done

def verify_designs(params, lib_name, impl_name, client, ready=False):
    results = {}
    for tb_name, tb_params in params['sim_params'].items():
        tb_cell_name = f'tb_{tb_name}'
        sch_done = client.submit(generate_schematic, tb_params, lib_name, tb_cell_name, ready=ready)
        results[tb_name] = client.submit(run_simulation, tb_params, lib_name, tb_cell_name,
                                         ready=sch_done)
    all_done = client.submit(all, [list(results.values())])
    return results, all_done

def characterize(params: Dict[str, Any], lib_name: str, impl_name: str, client):
    creation_done = create_designs(params, lib_name, impl_name, client)
    results, all_done = verify_designs(params, lib_name, impl_name, client, ready=creation_done)
    return results, all_done
