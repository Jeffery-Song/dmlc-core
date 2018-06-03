#!/usr/bin/env python
"""
DMLC submission script by ssh

One need to make sure all slaves machines are ssh-able.
"""
from __future__ import absolute_import

import os, subprocess, logging
from threading import Thread
from . import tracker

def sync_dir(local_dir, slave_node, slave_dir):
    """
    sync the working directory from root node into slave node
    """
    remote = slave_node[0] + ':' + slave_dir
    logging.info('rsync %s -> %s', local_dir, remote)
    prog = 'rsync -az --rsh="ssh -o StrictHostKeyChecking=no -p %s" %s %s' % (
        slave_node[1], local_dir, remote)
    subprocess.check_call([prog], shell = True)

def get_env(pass_envs):
    envs = []
    # get system envs
    keys = ['OMP_NUM_THREADS', 'KMP_AFFINITY', 'LD_LIBRARY_PATH', 'AWS_ACCESS_KEY_ID',
            'AWS_SECRET_ACCESS_KEY', 'DMLC_INTERFACE']
    for k in keys:
        v = os.getenv(k)
        if v is not None:
            envs.append('export ' + k + '=' + v + ';')
    # get ass_envs
    for k, v in pass_envs.items():
        envs.append('export ' + str(k) + '=' + str(v) + ';')
    return (' '.join(envs))

def submit(args):
    assert args.host_file is not None
    with open(args.host_file) as f:
        tmp = f.readlines()
    assert len(tmp) > 0
    hosts=[]
    for h in tmp:
        if len(h.strip()) > 0:
            # parse addresses of the form ip:port
            h = h.strip()
            i = h.find(":")
            p = "22"
            if i != -1:
                p = h[i+1:]
                h = h[:i]
            # hosts now contain the pair ip, port
            hosts.append((h, p))

    def ssh_submit(nworker, nserver, pass_envs):
        """
        customized submit script
        """
        # print(args)
        pass_envs['BROKERS'] = args.brokers # gbxu
        pass_envs['ADD_EPOCH'] = args.add_epoch
        pass_envs['MXNET_CMD'] = '"' + ' '.join(args.command) + '"'
        # switched_hosts = hosts[(nworker + nserver) % len(hosts):].append(hosts[:(nworker + nserver) % len(hosts)])
        # hosts_ip = 
        pass_envs['MXNET_WORKERS'] = ','.join([host[0] for host in hosts])
        # print('MXNET_CMD: ' + pass_envs['MXNET_CMD'])
        # print('ADD_EPOCH: ' + args.add_epoch)
        # print('BROKERS: ' + args.brokers)
        # thread func to run the job
        def run(prog):
            subprocess.check_call(prog, shell = True)

        # sync programs if necessary
        local_dir = os.getcwd()+'/'
        working_dir = local_dir
        if args.sync_dst_dir is not None and args.sync_dst_dir != 'None':
            working_dir = args.sync_dst_dir
            for h in hosts:
                sync_dir(local_dir, h, working_dir)

        pass_envs['MXNET_DIR'] = working_dir
        # launch jobs
        for i in range(nworker + nserver):
            pass_envs['DMLC_ROLE'] = 'server' if i < nserver else 'worker'
            (node, port) = hosts[i % len(hosts)]
            prog = get_env(pass_envs) + ' cd ' + working_dir + '; ' + (' '.join(args.command))
            prog = 'ssh -o StrictHostKeyChecking=no ' + node + ' -p ' + port + ' \'' + prog + '\''
            # print(prog)
            thread = Thread(target = run, args=(prog,))
            thread.setDaemon(True)
            thread.start()

        return ssh_submit

    tracker.submit(args.num_workers, args.num_servers,
                   fun_submit=ssh_submit,
                   pscmd=(' '.join(args.command)),
                   hostIP=args.host_ip,
                   env={'BROKERS' : args.brokers})
