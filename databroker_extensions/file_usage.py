'''
This program goes through the metadata for
CHX beamline and extracts file usage
- parameters may be specified such as 
experiment type, detector type, user, etc.
'''
from databroker import Broker
import pandas as pd

import os

import datetime
import time
from time import mktime

from eiger_io.fs_handler import EigerHandler
from databroker.assets.handlers import AreaDetectorTiffHandler

from pymongo.errors import CursorNotFound
from collections import defaultdict

def file_sizes(db, since, until, plan=None, detector=None):
    '''
    This function searches for keys that are stored via filestore in a
    database, and gathers the SPEC id's from them.
    
    Parameters
    ----------
    db: databroker object
        specifies beamline to inspect chosen by user
    since: str e.g. yyyy-mm-dd
        data in which to start searching through metadata
    until: str e.g. yyyy-mm-dd
        data in which to end searching through metadata
    plan: str (Optional) default is None
        user can specify type of plan
    detector: str (Optional) default is None
        user can specify type of detector
    Returns
    -------
    time_size: dict
        each key is a timestamp where each value is a dictionary(properties)
        e.g. time_size = 
            {
             '2018-12-31 00:00:00':
                    { 
                      'file_size' : 34634234,
                      'file_last_accessed': '2017-07-11 07:45:34',
                      'file_last_modified': '2017-07-07 14:27:05'
                    }
            }
    '''
    FILESTORE_KEY = "FILESTORE:"
    time_size = dict()
    file_properties = dict()
    used_resources = set()
    timestamp = 0.0

    files = []
    if plan is not None:
        hdrs = db(since=since, until=until, plan_name=plan)
    else:
        hdrs = db(since=since, until=until)
    hdrs = iter(hdrs)
    while True:
        try:
            hdr = next(hdrs)  
            for stream_name in hdr.stream_names:
                events = hdr.events(stream_name=stream_name)
                events = iter(events)
                while True:
                    try:
                        event = next(events)
                        if "filled" in event:
                            # there are keys that may not be filled
                            for key, val in event['filled'].items():
                                if not val:
                                    # get the datum
                                    if key in event['data']:
                                        # if user specifies a detector name this code block will run
                                        if detector:
                                            if detector == key:
                                                datum_id = event['data'][key]
                                                try:
                                                    resource = db.reg.resource_given_datum_id(datum_id)
                                                except:
                                                    print('No datum found for resource: {}'.format(datum_id))
                                                resource_id = resource['uid']
                                                if resource_id in used_resources:
                                                    continue
                                                else:
                                                    used_resources.add(resource_id)
                                                    datum_gen = db.reg.datum_gen_given_resource(resource)
                                                    try:
                                                        datum_kwargs_list = [datum['datum_kwargs'] for datum in datum_gen]
                                                    except TypeError:
                                                        print('type error for resource: {}'.format(resource))
                                                        continue
                                                    timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(event['time'])))
                                                    timestamp = time.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
                                                    timestamp = datetime.datetime.fromtimestamp(mktime(timestamp))
                                                    try:
                                                        fh = db.reg.get_spec_handler(resource_id)
                                                    except OSError:
                                                        print('OS error for resource: {}'.format(resource))
                                                    try:
                                                        file_lists = fh.get_file_list(datum_kwargs_list)
                                                        file_size = get_file_size(file_lists)
                                                    except KeyError:
                                                        print('key error for datum datum kwargs: {}'.format(datum_kwargs_list))
                                                        file_size = 0.0
                                                    if not file_lists:
                                                        raise OSError("No files found for {}".format(datum_kwargs_list))
                                                    last_accessed = get_file_last_accessed(file_lists)
                                                    last_modified = get_file_last_mod(file_lists)
                                                    print(fh)
                                                    print("{0}:{1}".format(key, file_size))
                                                    print("There are {} files in this object".format(len(file_lists)))
                                                    print("Last mod:{} | Last accessed {}".format(last_modified, last_accessed))
                                                    print(timestamp)
                                                    file_properties['file_size'] = file_size
                                                    file_properties['file_last_accessed'] = last_accessed
                                                    file_properties['file_last_modified'] = last_modified
                                                    time_size[timestamp] = file_properties
                                                    break
                                        else:
                                            datum_id = event['data'][key]
                                            try:
                                                resource = db.reg.resource_given_datum_id(datum_id)
                                            except:
                                                print('No datum found for resource: {}'.format(datum_id))
                                            resource_id = resource['uid']
                                            if resource_id in used_resources:
                                                continue
                                            else:
                                                used_resources.add(resource_id)
                                                datum_gen = db.reg.datum_gen_given_resource(resource)
                                                try:
                                                    datum_kwargs_list = [datum['datum_kwargs'] for datum in datum_gen]
                                                except TypeError:
                                                    print('type error for resource: {}'.format(resource))
                                                    continue
                                                timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(event['time'])))
                                                timestamp = time.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
                                                timestamp = datetime.datetime.fromtimestamp(mktime(timestamp))
                                                try:
                                                    fh = db.reg.get_spec_handler(resource_id)
                                                except OSError:
                                                    print('OS error for resource: {}'.format(resource))

                                                try:
                                                    file_lists = fh.get_file_list(datum_kwargs_list)
                                                    file_size = get_file_size(file_lists)
                                                except KeyError:
                                                    print('key error for datum datum kwargs: {}'.format(datum_kwargs_list))
                                                    file_size = 0.0
                                                if not file_lists:
                                                    raise OSError("No files found for {}".format(datum_kwargs_list))
                                                last_accessed = get_file_last_accessed(file_lists)
                                                last_modified = get_file_last_mod(file_lists)
                                                print(fh)
                                                print("File usage: {}".format(file_size))
                                                print("There are {} files in this object".format(len(file_lists)))
                                                print("Last mod:{} | Last accessed {}".format(last_modified, last_accessed))
                                                print(timestamp)
                                                file_properties['file_size'] = file_size
                                                file_properties['file_last_accessed'] = last_accessed
                                                file_properties['file_last_modified'] = last_modified
                                                time_size[timestamp] = file_properties
                                                break
                    except StopIteration:
                        break
                    except KeyError:
                        print('key error')
                        continue
        except CursorNotFound:
            print('CursorNotFound = {}'.format(hdr))
            curr_time = hdr.start['time']+1
            tstruct = time.strptime(time.ctime(curr_time), "%a %b %d %H:%M:%S %Y")
            new_time = time.strftime("%Y-%m-%d %H:%M:%S", tstruct)
            hdrs = iter(db(since=since, until=new_time))
            print("Restarting up to {new_time}".format(new_time=new_time))
        except StopIteration:
            break
    return time_size

def get_file_size(file_list):
    '''
    returns file usage
    
    Parameters
    ----------
    file_list: list
        each file in the list will be inspected to extract file usage
    Returns
    -------
    sum(sizes): int
        file usage per file gets summed into an int
    '''
    sizes = []
    for file in file_list:
        try:
            sizes.append(os.path.getsize(file))
        except OSError:
          raise OSError("File not found at file path {}".format(file))
    return sum(sizes)

def get_file_last_mod(file_list):
    '''
    loops through list of files and checks to see which file was modified most recently
    Parameters
    ----------
    file_list: list
        each file in the list will be inspected to extract the timestamp of the file that was most recently modified 
    Returns
    -------
    last_modified: str
        timestamp of the most recent modified file in the list
    '''
    for i, file in enumerate(file_list):
        if os.path.isfile(file):
            try:
                file1 = os.path.getmtime(file)
                file2 = os.path.getmtime(file_list[i+1])
                if file1 > file2:
                    last_modified = time.ctime(file1)
                else:
                    last_modified = time.ctime(file2)
            except IndexError:
                print("Index out of bounds.") 
    return last_modified
            
def get_file_last_accessed(file_list):
    '''
    loops through list of files and checks to see which file was accessed most recently
    Parameters
    ----------
    file_list: list
        each file in the list will be inspected to extract the timestamp of the file that was most recently accessed 
    Returns
    -------
    last_modified: str
        timestamp of the most recent accessed file in the list
    '''
    for i, file in enumerate(file_list):
        if os.path.isfile(file):
            try:
                file1 = os.stat(file).st_atime
                file2 = os.stat(file_list[i+1]).st_atime
                if file1 > file2:
                    last_accessed = time.ctime(file1)
                else:
                    last_accessed = time.ctime(file2)
            except IndexError:
                print("Index out of bounds.") 
    return last_accessed

