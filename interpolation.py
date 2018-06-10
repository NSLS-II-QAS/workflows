# Set up a Broker.

from databroker.assets.handlers_base import HandlerBase


# Import other stuff
import socket
from pathlib import Path
import os
import os.path as op
from subprocess import call
import json
import pickle
import pandas as pd
import numpy as np
import pwd
import grp

import time as ttime

# lightflow stuff
from lightflow.models import Dag, Action, Parameters, Option
from lightflow.tasks import PythonTask

# Set up zeromq sockets
import zmq
import socket
#context = zmq.Context()
machine_name = socket.gethostname()

import kafka

# Create PULLER to receive information from workstations
#receiver = context.socket(zmq.PULL)
#receiver.connect("tcp://xf08id-srv2:5560")

# Create PUSHER to send information back to workstations

#Setup beamline specifics:
beamline_gpfs_path = '/nsls2/xf07bm/'
user_data_path = beamline_gpfs_path + 'users/'

# Set up logging.
import logging
import logging.handlers

def get_logger():
    logger = logging.getLogger('worker_srv_lightflow')
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # only add handlers if not added before
    if not len(logger.handlers):
        logger.setLevel(logging.DEBUG)
        # Write DEBUG and INFO messages to /var/log/data_processing_worker/debug.log.
        debug_file = logging.handlers.RotatingFileHandler(
            beamline_gpfs_path + '/users/log/{}_data_processing_lightflow_debug.log'.format(machine_name),
            maxBytes=10000000, backupCount=9)
        debug_file.setLevel(logging.DEBUG)
        debug_file.setFormatter(formatter)
        logger.addHandler(debug_file)
    
        # Write INFO messages to /var/log/data_processing_worker/info.log.
        info_file = logging.handlers.RotatingFileHandler(
            beamline_gpfs_path + '/users/log/{}_data_processing_lightflow_info.log'.format(machine_name),
            maxBytes=10000000, backupCount=9)
        info_file.setLevel(logging.INFO)
        info_file.setFormatter(formatter)
        logger.addHandler(info_file)
        

    return logger


from databroker import Broker
from isstools.xiaparser import xiaparser
from isstools.xasdata import xasdata

class ScanProcessor():
    def __init__(self, dbname, beamline_gpfs_path, username, 
                 pulses_per_deg=360000, topic="qas-processing",
                 bootstrap_servers=['cmb01:9092', 'cmb02:9092'],
                 *args, **kwargs):
        # these can't be pickled
        self.logger = get_logger()
        self.logger.info("Begin scan processor")
        db = Broker.named(dbname)
        db_analysis = Broker.named('qas-analysis')
        # Set up isstools parsers

        # TODO: fix pulses per deg
        gen_parser = xasdata.XASdataGeneric(pulses_per_deg, db, db_analysis)
        xia_parser = xiaparser.xiaparser()

        self.gen_parser = gen_parser
        self.xia_parser = xia_parser
        self.db = db
        self.md = {}
        self.root_path = Path(beamline_gpfs_path)
        self.user_data_path = Path(beamline_gpfs_path) / Path('users')
        self.xia_data_path = Path(beamline_gpfs_path) / Path('xia_files')
        self.uid = pwd.getpwnam(username).pw_uid
        self.gid = grp.getgrnam(username).gr_gid

        # TODO : move this in a separate function? (Julien)
        #context = zmq.Context()
        #self.sender = context.socket(zmq.PUSH)
        self.publisher = kafka.KafkaProducer(bootstrap_servers=bootstrap_servers)
        self.topic = topic

        # by default we send to srv2
        self.logger.info("Sending request to server")
        #self.sender.connect("tcp://xf07bm-ws1:5561")

    def process(self, md, requester, interp_base='i0'):
        current_path = self.create_user_dirs(self.user_data_path,
                                             md['year'],
                                             md['cycle'],
                                             md['PROPOSAL'])
        try:
            current_filepath = Path(current_path) / Path(md['name'])
            current_filepath = ScanProcessor.get_new_filepath(str(current_filepath) + '.hdf5')
            current_uid = md['uid']
            self.gen_parser.load(current_uid)
        except:
            self.logger.info("md['name'] not set")
            pass
        

        self.logger.info("Processing started for %s", md['uid'])
        if 'plan_name' in md:
            if md['plan_name'] == 'get_offsets':
                pass
            elif md['plan_name'] == 'execute_trajectory' or md['plan_name'] == 'execute_xia_trajectory':
                self.logger.info("Interpolation started for %s", md['uid'])
                
                if md['plan_name'] == 'execute_trajectory':
                    self.process_tscan(interp_base)
                elif md['plan_name'] == 'execute_xia_trajectory':
                    self.process_tscanxia(md, current_filepath)
                
                division = self.gen_parser.interp_df['i0'].values / self.gen_parser.interp_df['it'].values
                division[division < 0] = 1

                filename = self.gen_parser.export_trace_hdf5(current_filepath[:-5], '')
                os.chown(filename, self.uid, self.gid)

                filename = self.gen_parser.export_trace(current_filepath[:-5], '')
                os.chown(filename, self.uid, self.gid)

                self.logger.info('Interpolated file %s stored to ', filename)

                ret = create_ret('spectroscopy', current_uid, 'interpolate', filename,
                                 md, requester)
                #self.sender.send(ret)
                future = self.publisher.send(self.topic, ret)
                result = future.get(timeout=60)
                self.logger.info('Interpolation of %s complete', filename)
                self.logger.info('Binning of %s started', filename)
                e0 = int(md['e0'])
                bin_df = self.gen_parser.bin(e0, e0 - 30, e0 + 30, 4, 0.2, 0.04)

                filename = self.gen_parser.data_manager.export_dat(current_filepath[:-5]+'.hdf5', e0)
                print(f"current_filepath: {current_filepath[:-5] + '.hdf5'}")
                os.chown(filename, self.uid, self.gid)
                self.logger.info('Binning of %s complete', filename)
                
                
                ret = create_ret('spectroscopy', current_uid, 'bin', filename, md, requester)
                #self.sender.send(ret)
                # need to wait before exiting
                future = self.publisher.send(self.topic, ret)
                result = future.get(timeout=60)
                self.logger.info("Processing complete for %s", md['uid'])

                
                #store_results_databroker(md,
                #                         parent_uid,
                #                         db_analysis,
                #                         'interpolated',
                #                         current_filepath[:-5] + '.hdf5',
                #                         root='')
            elif md['plan_name'] == 'relative_scan':
                pass

    def bin(self, md, requester, proc_info, filepath=''):
        self.logger.info("Binning started for %s", md['uid'])
        print('starting binning!', md['uid'])
        if filepath is not '':
            current_filepath = filepath
        else:
            current_path = self.create_user_dirs(self.user_data_path,
                                                 md['year'],
                                                 md['cycle'],
                                                 md['PROPOSAL'])
            current_filepath = str(Path(current_path) / Path(md['name'])) + '.txt'
        self.gen_parser.loadInterpFile(str(current_filepath))
        self.logger.info("Filepath %s", current_filepath)
        e0 = proc_info['e0']
        edge_start = proc_info['edge_start']
        edge_end = proc_info['edge_end']
        preedge_spacing = proc_info['preedge_spacing']
        xanes_spacing = proc_info['xanes_spacing']
        exafs_spacing = proc_info['exafs_spacing']
        bin_df = self.gen_parser.bin(e0, e0 + edge_start, e0 + edge_end, preedge_spacing, xanes_spacing, exafs_spacing)

        filename = self.gen_parser.data_manager.export_dat(f'{str(current_filepath)}', e0)
        
        os.chown(filename, self.uid, self.gid)
        ret = create_ret('spectroscopy', md['uid'], 'bin', filename, md, requester)
        self.logger.info('File %s binned', filename)
        future = self.publisher.send(self.topic, ret)
        result = future.get(timeout=60)
        # WARNING: We NEED this sleep!
        # There seems to be a bug with pykafka!!!!
        ttime.sleep(1)
        self.logger.info("Binning complete for %s", md['uid'])
        print(os.getpid(), 'Done with the binning!') 

    def return_interp_data(self, md, requester, filepath=''):
        logger = get_logger()
        logger.info("Processor: preparing to return interpolated data")
        if filepath is not '':
            current_filepath = filepath
        else:
            current_path = self.create_user_dirs(self.user_data_path,
                                                 md['year'],
                                                 md['cycle'],
                                                 md['PROPOSAL'])
            current_filepath = str(Path(current_path) / Path(md['name'])) + '.txt'
        logger.info("Processor: reading interp file : {}".format(str(current_filepath)))
        self.gen_parser.loadInterpFile(f'{str(current_filepath)}')
        logger.info("Processor: loaded. Preparing return to send")
        ret = create_ret('spectroscopy', md['uid'], 'request_interpolated_data', current_filepath, md, requester)
        logger.info("Processor: sending back the interpolated data from request to topic {}".format(self.topic))
        #from celery.contrib import rdb
        #rdb.set_trace()
        future = self.publisher.send(self.topic, ret)
        result = future.get(timeout=60)
        # WARNING: We NEED this sleep!
        # There seems to be a bug with pykafka!!!!
        ttime.sleep(1)
        logger.info("Processor: sent")


    def process_tscan(self, interp_base='i0'):
        print('Processing tscan')
        self.gen_parser.interpolate(key_base=interp_base)

    def process_tscanxia(self, md, current_filepath):
        # Parse xia
        print('Processing: xia scan')
        self.gen_parser.interpolate(key_base='xia_trigger')
        xia_filename = md['xia_filename']
        # this should not run
        raise ValueError("Should not get here (QAS)")
        xia_filepath = 'smb://xf07bm-nas1/xia_data/{}'.format(xia_filename)
        xia_destfilepath = Path(self.xia_data_path) / Path(xia_filename)
#        xia_destfilepath = '{}{}'.format(self.xia_data_path, xia_filename)
        smbclient = xiaparser.smbclient(xia_filepath, str(xia_destfilepath))
        try:
            smbclient.copy()
        except Exception as exc:
            if exc.args[1] == 'No such file or directory':
                print('*** File not found in the XIA! Check if the hard drive is full! ***')
            else:
                print(exc)
            print('Abort current scan processing!\nDone!')
            return

        interp_base = 'xia_trigger'
        self.gen_parser.interpolate(key_base=interp_base)
        xia_parser = self.xia_parser
        xia_parser.parse(xia_filename, self.xia_data_path)
        xia_parsed_filepath = current_filepath[0: current_filepath.rfind('/') + 1]
        xia_parser.export_files(dest_filepath=xia_parsed_filepath, all_in_one=True)

        try:
            if xia_parser.channelsCount():
                length = min(xia_parser.pixelsCount(0), len(self.gen_parser.interp_arrays['energy']))
                if xia_parser.pixelsCount(0) != len(self.gen_parser.interp_arrays['energy']):
                    len_xia = xia_parser.pixelsCount(0)
                    len_pb = len(self.gen_parser.interp_arrays['energy'])
                    raise Exception('XIA Pixels number ({}) != '
                                    'Pizzabox Trigger number ({})'.format(len_xia, len_pb))
            else:
                raise Exception("Could not find channels data in the XIA file")
        except Exception as exc:
            print('***', exc, '***')

        mcas = []
        if 'xia_rois' in md:
            xia_rois = md['xia_rois']
            if 'xia_max_energy' in md:
                xia_max_energy = md['xia_max_energy']
            else:
                xia_max_energy = 20

            for mca_number in range(1, xia_parser.channelsCount() + 1):
                if 'xia1_mca{}_roi0_high'.format(mca_number) in xia_rois:
                    rois_array = []
                    roi_numbers = [roi_number for roi_number in
                                   [roi.split('mca{}_roi'.format(mca_number))[1].split('_high')[0] for roi in
                                   xia_rois if len(roi.split('mca{}_roi'.format(mca_number))) > 1] if
                                   len(roi_number) <= 3]
                    for roi_number in roi_numbers:
                        rois_array.append(
                            [xia_rois['xia1_mca{}_roi{}_high'.format(mca_number, roi_number)],
                             xia_rois['xia1_mca{}_roi{}_low'.format(mca_number, roi_number)]])
                    mcas.append(xia_parser.parse_roi(range(0, length), mca_number, rois_array, xia_max_energy))
                else:
                    mcas.append(xia_parser.parse_roi(range(0, length), mca_number, [
                        [xia_rois['xia1_mca1_roi0_low'], xia_rois['xia1_mca1_roi0_high']]], xia_max_energy))

            for index_roi, roi in enumerate([[i for i in zip(*mcas)][ind] for ind, k in enumerate(roi_numbers)]):
                xia_sum = [sum(i) for i in zip(*roi)]
                if len(self.gen_parser.interp_arrays['energy']) > length:
                    xia_sum.extend([xia_sum[-1]] * (len(self.gen_parser.interp_arrays['energy']) - length))
                roi_label = ''
                #roi_label = getattr(self.parent_gui.widget_sdd_manager, 'edit_roi_name_{}'.format(roi_numbers[index_roi])).text()
                if not len(roi_label):
                    roi_label = 'XIA_ROI{}'.format(roi_numbers[index_roi])

                self.gen_parser.interp_df[roi_label] = np.array([xia_sum]).transpose()

                #self.gen_parser.interp_arrays[roi_label] = np.array(
                #    [self.gen_parser.interp_arrays['energy'][:, 0], xia_sum]).transpose()

                #self.figure.ax.plot(self.gen_parser.interp_arrays['energy'][:, 1], -(
                #    self.gen_parser.interp_arrays[roi_label][:, 1] / self.gen_parser.interp_arrays['i0'][:, 1]))

    def create_user_dirs(self, user_data_path, year, cycle, proposal):
        current_user_dir = Path(f"{year}.{cycle}.{proposal}")

        user_data_path = Path(user_data_path) / current_user_dir
        ScanProcessor.create_dir(user_data_path)

        log_path = user_data_path / Path('log')
        ScanProcessor.create_dir(log_path)

        snapshots_path = log_path / Path('snapshots')
        ScanProcessor.create_dir(snapshots_path)

        return user_data_path

    def get_new_filepath(filepath):
        if op.exists(Path(filepath)):
            if filepath[-5:] == '.hdf5':
                filepath = filepath[:-5]
            iterator = 2

            while True:
                new_filepath = f'{filepath}-{iterator}.hdf5'
                if not op.isfile(new_filepath):
                    return new_filepath
                iterator += 1
        return filepath

    def create_dir(path):
        if not op.exists(path):
            os.makedirs(path)
            call(['setfacl', '-m', 'g:qas-staff:rwx', path])
            call(['chown', '-R', 'xf07bm:xf07bm', path ])
            # meant to be run static
            logger = get_logger()
            logger.info("Directory %s created succesfully", path)


def create_ret(scan_type, uid, process_type, data, metadata, requester):
    '''
        Create a return.
        Can also be a file path
    '''
    if hasattr(data, 'to_msgpack'):
        data = data.to_msgpack(compress='zlib')
    else:
        # not a df, just string
        data = data.encode()
    ret = {'type':scan_type,
           'uid': uid,
           'processing_ret':{
                             'type': process_type,
                             'data': data,
                             'metadata': metadata
                            }
          }

    return (requester.encode() + pickle.dumps(ret))


# required parameters for the call
parameters = Parameters([
        Option('request', help='Specify a uid', type=dict),
        ])


def create_ret_func(scan_type, uid, process_type, data, metadata, requester):
    ret = {'type':scan_type,
           'uid': uid,
           'processing_ret':{
                             'type':process_type,
                             'data':data,
                             'metadata': metadata
                            }
          }

    return (requester + pickle.dumps(ret)).encode()


def process_run_func(data, store, signal, context):
    #sender_host = "tcp://xf08id-srv2:5561"
    logger = get_logger()
    logger.info("Starting ScanProcessor....")

    request = store.get('request')

    pulses_per_deg = request.get('pulses_per_deg')
    processor = ScanProcessor("qas", beamline_gpfs_path, 'xf07bm',
                              pulses_per_deg=pulses_per_deg)
    db = Broker.named("qas")
    

    #data = json.loads(receiver.recv().decode('utf-8'))
    #logger.debug("request : %s", request)
    #from celery.contrib import rdb
    #rdb.set_trace()
    uid = request['uid']

    md = db[uid].start

    if request['type'] == 'spectroscopy':
        process_type = request['processing_info']['type']

        start_doc = md 
        if process_type == 'interpolate':
            logger.info("interpolating (not performed yet)")
            #from celery.contrib import rdb
            #rdb.set_trace()
            processor.process(start_doc, requester=request['requester'], interp_base=request['processing_info']['interp_base'])
           
        elif process_type == 'bin':
            logger.info("binning (not performed yet)")
            processor.bin(start_doc, requester=request['requester'], proc_info=request['processing_info'], filepath=request['processing_info']['filepath'])

        elif process_type == 'request_interpolated_data':
            logger.info("returning interpolated data (not done yet)")
            processor.return_interp_data(start_doc, requester=request['requester'], filepath=request['processing_info']['filepath'])



# don't create the request anymore
#create_req_task = PythonTask(name="create_req_func", callback=create_req_func,
                             #queue='qas-task')
process_run_task = PythonTask(name="process_run_func",
                              callback=process_run_func, queue='qas-task')


d = Dag("interpolation", queue="qas-dag")
d.define({
    process_run_task: None,
    })
