import pandas as pd
import numpy as np
import binascii as ba


class HelperFunctions:
    @staticmethod
    def divide_by_zero(df, column, divisor):
        try:
            df[column] = df[column] / divisor
        except ZeroDivisionError:
            df[column] = 0

    @staticmethod
    def read_csv(filename, group):
        group_list = []
        with open(filename) as file:
            for line in file:
                line = line.strip("\0\n")
                if len(line) > 0:
                    if group == "cgroup":
                        if line.split(",")[1].split("/")[1] == 'cgroup':
                            group_list.append(line)
                    else:
                        if line.split(",")[1].split("/")[1] != 'cgroup':
                            group_list.append(line)
        return group_list


class ProcessData:
    """
    PROCESS INFORMATION
    timestamp   = time and date
    desc        = step / process name
    pid         = pid
    state       = process state
    ppid        = parent pid
    nthrds      = number of threads of the process group
    rss         = resident set size
    pagesize    = page size
    rss_kb      = derived from rss and pagesize
    pss_kb      = unused
    vsize       = virtual size
    proc        = running processor
    avg_cpu     = average total cpu time
    avg_usr     = average user cpu time
    avg_sys     = average system cpu time
    secs        = duration in seconds
    cputime     = cumulative total cpu time
    usrtime     = cumulative user cpu time
    systime     = cumulative system cpu time
    rchar       = cumulative character reads (includes terminal I/O and may of not actually
                  resulted in physical disk I/O)
    rbytes      = cumulative bytes read (I/Os fetched from the storage layer - accurate
                  for block-backed filesystems)
    syscr       = cumulative system call reads (read, pread, etc.)
    rsize       = rbytes / syscr
    wchar       = cumulative character writes (includes terminal I/O and may of not actually
                  resulted in physical disk I/O)
    wbytes      = cumulative bytes written (I/Os fetched from the storage layer - accurate
                  for block-backed filesystems)
    syscw       = cumulative system call writes (write, pwrite, etc.)
    wsize       = wbytes / syscw
    """
    @staticmethod
    def build_process_dataframe(group_list):
        df = pd.DataFrame(data=[line.split(",") for line in group_list],
                          columns=['timestamp', 'desc', 'pid', 'state', 'ppid', 'nthrds', 'rss',
                                   'pagesize', 'rss_kb', 'pss_kb', 'vsize', 'proc',
                                   'avg_cpu', 'avg_usr', 'avg_sys', 'secs', 'cputime', 'usrtime',
                                   'systime', 'rchar', 'rbytes', 'syscr', 'rsize', 'wchar',
                                   'wbytes', 'syscw', 'wsize'])

        # Convert object to data types
        dtype = {'timestamp': object, 'desc': str, 'pid': int, 'state': str, 'ppid': int, 'nthrds': int,
                 'rss': 'int64', 'pagesize': 'int64', 'rss_kb': 'int64', 'pss_kb': 'int64', 'vsize': 'float64',
                 'proc': int, 'avg_cpu': 'float64', 'avg_usr': 'float64',
                 'avg_sys': 'float64', 'secs': 'int64', 'cputime': 'float64', 'usrtime': 'float64',
                 'systime': 'float64', 'rchar': 'int64', 'rbytes': 'int64', 'syscr': 'int64', 'rsize': float,
                 'wchar': 'int64', 'wbytes': 'int64', 'syscw': 'int64', 'wsize': float}
        for idx, obj in dtype.items():
            df[idx] = df[idx].astype(obj)

        # Convert timestamp from %Y/%m/%dT%H:%M:%S to %Y/%m/%d-%H:%M:%S
        df['timestamp'] = pd.to_datetime(df['timestamp'],
                                         format='%Y/%m/%dT%H:%M:%S')  # .dt.strftime("%Y/%m/%d-%H:%M:%S")

        df.set_index(['timestamp'], inplace=True)

        df['uname'] = df['pid'].apply(str) + ":" + df['desc'] + ":" + df['ppid'].apply(str)
        df['uname'] = df['uname'].apply(lambda x: ba.hexlify(x.encode()).decode())

        iobins = [-1, 0, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288,
                  1048576, 2097152, 4194304, 8388608]
        iolabels = ['0B', '512B', '1K', '2K', '4K', '8K', '16K', '32K', '64K', '128K', '256K', '512K',
                    '1M', '2M', '4M', '8M']
        df['rsize_bin'] = pd.cut(df['rsize'], bins=iobins, labels=iolabels)
        df['wsize_bin'] = pd.cut(df['wsize'], bins=iobins, labels=iolabels)
        df['rsize_bin'] = df['rsize_bin'].fillna('0B')
        df['wsize_bin'] = df['wsize_bin'].fillna('0B')

        # Convert rss from pages to kilobytes
        df['rss_kb'] = df['rss'] * (df['pagesize'] / 1024)
        # Convert bytes to kilobytes
        HelperFunctions.divide_by_zero(df, 'vsize', 1024)
        HelperFunctions.divide_by_zero(df, 'rchar', 1024)
        HelperFunctions.divide_by_zero(df, 'rbytes', 1024)
        HelperFunctions.divide_by_zero(df, 'wchar', 1024)
        HelperFunctions.divide_by_zero(df, 'wbytes', 1024)

        # next bit replace the following two lines, not sure why this version of Pandas (0.22.0) doesn't like it
        # df.replace([np.inf, np.nan], 0, inplace=True)
        df = df.apply(pd.Series.replace, to_replace=np.inf, value=0)
        df = df.apply(pd.Series.replace, to_replace=np.nan, value=0)

        return df

    """
    CGROUP INFORMATION
    timestamp     = time and date
    desc          = step / process name
    tgids         = number of thread groups associated with the step
    pids          = number of pids/lwps associated with the step
    cache         = page cache in bytes
    rss           = resident set size in bytes
    mapped_file   = memory mapped files in bytes
    inactive_anon = inactive anonymous in bytes
    active anon   = active anonymous in bytes
    unevictable   = unevictable memory in bytes
    tcache        = page cache including children in bytes
    trss          = rss including children in bytes 
    """
    @staticmethod
    def build_cgroup_dataframe(group_list):
        df = pd.DataFrame(data=[line.split(",") for line in group_list],
                          columns=['timestamp', 'desc', 'tgids', 'pids', 'cache', 'rss', 'mapped_file',
                                   'inactive_anon', 'active_anon', 'unevictable', 'tcache', 'trss'])

        # Convert object to data types
        dtype = {'timestamp': object, 'desc': str, 'tgids': int, 'pids': int, 'cache': 'int64', 'rss': 'int64',
                 'mapped_file': 'int64', 'inactive_anon': 'int64', 'active_anon': 'int64', 'unevictable': 'int64',
                 'tcache': 'int64', 'trss': 'int64'}
        for idx, obj in dtype.items():
            df[idx] = df[idx].astype(obj)

        # Convert timestamp from %Y/%m/%dT%H:%M:%S to %Y/%m/%d-%H:%M:%S
        df['timestamp'] = pd.to_datetime(df['timestamp'],
                                         format='%Y/%m/%dT%H:%M:%S')  # .dt.strftime("%Y/%m/%d-%H:%M:%S")

        # Convert bytes to kilobytes
        HelperFunctions.divide_by_zero(df, 'cache', 1024)
        HelperFunctions.divide_by_zero(df, 'rss', 1024)
        HelperFunctions.divide_by_zero(df, 'tcache', 1024)
        HelperFunctions.divide_by_zero(df, 'trss', 1024)

        df.set_index(['timestamp'], inplace=True)

        # next bit replace the following two lines, not sure why this version of Pandas (0.22.0) doesn't like it
        # df.replace([np.inf, np.nan], 0, inplace=True)
        df = df.apply(pd.Series.replace, to_replace=np.inf, value=0)
        df = df.apply(pd.Series.replace, to_replace=np.nan, value=0)

        return df
