from simple_salesforce import Salesforce
import string
import requests
import wave
import audioop
import subprocess
import struct

region_name = 'NA SouthEast' #Salesforce string for region
local_dir_prefix = 'audio/' #put the raw audio here
dest_hdfs_dir = '/user/nifi/audio/in/' #copy the raw audio here

def run_cmd(args_list):
    """
    run linux commands
    """
    # import subprocess
    print('Running system command: {0}'.format(' '.join(args_list)))
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    s_output, s_err = proc.communicate()
    s_return =  proc.returncode
    return s_return, s_output, s_err

def getHeaders(bufHeader):
    stHeaderFields = {'ChunkSize' : 0, 'Format' : '',
        'Subchunk1Size' : 0, 'AudioFormat' : 0,
        'NumChannels' : 1, 'SampleRate' : 8000,
        'ByteRate' : 0, 'BlockAlign' : 0,
        'BitsPerSample' : 16, 'Filename': ''}
    # Verify that the correct identifiers are present
    if (bufHeader[0:4] != "RIFF") or \
       (bufHeader[12:16] != "fmt "):
         print "not a standard wav file"
         return stHeaderFields
    # endif
    # Parse fields
    stHeaderFields['ChunkSize'] = struct.unpack('<L', bufHeader[4:8])[0]
    stHeaderFields['Format'] = bufHeader[8:12]
    stHeaderFields['Subchunk1Size'] = struct.unpack('<L', bufHeader[16:20])[0]
    stHeaderFields['AudioFormat'] = struct.unpack('<H', bufHeader[20:22])[0]
    stHeaderFields['NumChannels'] = struct.unpack('<H', bufHeader[22:24])[0]
    stHeaderFields['SampleRate'] = struct.unpack('<L', bufHeader[24:28])[0]
    stHeaderFields['ByteRate'] = struct.unpack('<L', bufHeader[28:32])[0]
    stHeaderFields['BlockAlign'] = struct.unpack('<H', bufHeader[32:34])[0]
    stHeaderFields['BitsPerSample'] = struct.unpack('<H', bufHeader[34:36])[0]
    return stHeaderFields

with open('salesforce_login.txt') as f:
    username, password, token = [x.strip("\n") for x in f.readlines()]
sf = Salesforce(username=username, password=password, security_token=token)

lead_ids = sf.query("select id from lead where region__c='"+region_name+"'")


for lead_rec in lead_ids['records']:
    lead_id = lead_rec['Id']
    histories = sf.query("select id,(select id,activity_type__c,subject,description from activityhistories order by activitydate desc, lastmodifieddate desc limit 100) from lead where id='"+lead_id+"'")
    #print(histories['records'][0]['ActivityHistories'])
    if histories['records'][0]['ActivityHistories'] is None:
        continue
    for activityrec in histories['records'][0]['ActivityHistories']['records']:
        if activityrec['Activity_Type__c'] != 'Call':
            continue

        desc = activityrec['Description']
        if desc is None or desc.find('https:') < 0:
            continue

        history_id = activityrec['Id']
        print("History ID: "+history_id)

        url = desc[desc.index('https:'):]
        print("Audio URL: "+url)

        filename = local_dir_prefix+history_id+'.wav'
        r = requests.get(url)
        f2 = open(filename+'.orig','wb')
        f2.write(r.content)
        f2.close()

        headerInfo = getHeaders(r.content[:38])

        f = wave.open(filename,'wb')
        f.setnchannels(1)
        f.setsampwidth(headerInfo['BitsPerSample']/8)
        f.setframerate(headerInfo['SampleRate'])
        if (headerInfo is not None and headerInfo['NumChannels'] == 2):
            monoized = audioop.tomono(r.content, 2, 0.5, 0.5)
            f.writeframes(monoized)
        else:
            f.writeframes(r.content)
        f.close()

        (ret, out, err) = run_cmd(['hdfs', 'dfs', '-put', filename, dest_hdfs_dir])
        print ret, out, err
