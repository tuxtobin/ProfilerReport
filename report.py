import argparse
import os
import logging
from Helpers import HelperFunctions
from Helpers import ProcessData
from Graphers import MatplotlibGraphs as mg
from fpdf import FPDF


# setup logging
logger = logging.getLogger('Profiler Report')
logger.setLevel(logging.DEBUG)
log_handler = logging.StreamHandler()
log_handler.setLevel(logging.DEBUG)
log_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log_handler.setFormatter(log_format)
logger.addHandler(log_handler)

logger.info('Starting ...')

# pull in arguments
logger.info('Reading arguments')
ap = argparse.ArgumentParser()
ap.add_argument('-v', '--version', action='version', version='Version 1.0 - Superb Squid')
ap.add_argument('-i', '--input', required=True, type=str, help='Input profiler file')
args = ap.parse_args()

# check that the input file exists if not exit
if not os.path.exists(args.input):
    logger.error('Input file does not exist')
    exit(99)

process_list = HelperFunctions.read_csv(filename=args.input, group="process")
process_df = ProcessData.build_process_dataframe(group_list=process_list)
process_list.clear()

logger.info('Plotting CPU Usage (Sum)')
mg.line_summary(df=process_df, field='avg_cpu', title='CPU Usage (Sum)', y='CPU Avg')

logger.info('Plotting RSS Usage (Sum)')
process_df['rss_orig'] = process_df['rss']
process_df['rss'] = process_df['rss'] / (1024 * 1024)
mg.line_summary(df=process_df, field='rss', title='RSS Usage (Sum)', y='GBytes')

logger.info('Plotting CPU User & System Usage')
mg.line_detail(df=process_df, fields=['avg_sys', 'avg_usr'], title='CPU User & System Usage', y='CPU Avg')

logger.info('Plotting CPU User & System Stacked Usage')
mg.stack_summary(df=process_df, fields=['avg_sys', 'avg_usr'], title='CPU User & System Stacked Usage', y='CPU Avg')

logger.info('Plotting RSS & VSize Usage')
process_df['vsize_orig'] = process_df['vsize']
process_df['vsize'] = process_df['vsize'] / (1024 * 1024)
mg.line_detail(df=process_df, fields=['rss', 'vsize'], title='RSS & VSize Usage', y='GBytes')

logger.info('Plotting RSS & VSize Stacked Usage')
mg.stack_summary(df=process_df, fields=['rss', 'vsize'], title='RSS & VSize Stacked Usage', y='GBytes')

logger.info('Plotting RChar & WChar Activity')
mg.line_detail(df=process_df, fields=['rchar', 'wchar'], title='IO Activity', y='Kbytes', diff=True)

logger.info('Plotting RChar & WChar Stacked Activity')
mg.stack_summary(df=process_df, fields=['rchar', 'wchar'], title='IO Stacked Activity', y='Kbytes', diff=True)

logger.info('Plotting RBytes & WBytes Activity')
mg.line_detail(df=process_df, fields=['rbytes', 'wbytes'], title='Bytes Read & Written to Storage',
               y='Kbytes', diff=True)

logger.info('Plotting RBytes & WBytes Stacked Activity')
mg.stack_summary(df=process_df, fields=['rbytes', 'wbytes'], title='Stacked Bytes Read & Written to Storage',
               y='Kbytes', diff=True)

logger.info('Plotting Read & Write System Call Activity')
mg.line_detail(df=process_df, fields=['syscr', 'syscw'], title='Read & Write System Call Activity',
               y='#System Calls', diff=True)

logger.info('Plotting Read & Write System Call Stacked Activity')
mg.stack_summary(df=process_df, fields=['syscr', 'syscw'], title='Read & Write System Call Stacked Activity',
                 y='#System Calls', diff=True)

logger.info('Plotting CPU Time (Sum)')
mg.line_summary(df=process_df, field='cputime', title='CPU Time (Sum)', y='Time', diff=True)

logger.info('Plotting CPU Process State')
mg.broken_barh(df=process_df, fields=['proc', 'state'], title='CPU Process State', y='CPU')

read_dict = process_df['rsize_bin'].value_counts().to_dict()
write_dict = process_df['wsize_bin'].value_counts().to_dict()
read_list = []
write_list = []
io_dict = dict()
io_dict['columns'] = ['0B', '512B', '1K', '2K', '4K', '8K', '16K', '32K', '64K', '128K', '256K', '512K',
                      '1M', '2M', '4M', '8M']
for key in io_dict['columns']:
    read_list.append(read_dict[key]) if key in read_dict else read_list.append(0)
    write_list.append(write_dict[key]) if key in write_dict else write_list.append(0)
io_dict['Read'] = read_list
io_dict['Write'] = write_list
logger.info('Plotting I/O Distribution')
mg.bar_detail(data=io_dict, title='IO Distribution', y='IO Frequency', x='IO Sizes')
