import matplotlib.pyplot as plt
from matplotlib import dates
from matplotlib.dates import DateFormatter
from pandas.plotting import register_matplotlib_converters
import pandas as pd
import numpy as np
import binascii as ba
import matplotlib.dates as mdates


class MatplotlibGraphs:
    register_matplotlib_converters()
    plt.rcParams["figure.figsize"] = (15, 8)
    plt.rcParams["figure.dpi"] = 100
    plt.rcParams["savefig.dpi"] = 100
    plt.rcParams["legend.loc"] = 'upper left'

    @staticmethod
    def line_summary(df, field, title, y, x='Date/Time', interval=60, output='output', diff=False):
        fig, ax = plt.subplots(1, 1, sharex='all', sharey='all')

        grp = df.groupby(df.index)[field].sum().reset_index()
        grp.set_index(['timestamp'], inplace=True)
        if diff:
            grp[field] = grp[field].diff()
            grp = grp.apply(pd.Series.replace, to_replace=np.nan, value=0)
            grp[grp[field] < 0] = 0
            percentile = grp[field].quantile(0.9)
            grp[field].clip(0, percentile, inplace=True)

        ax.plot(grp.index, grp[field], color='r')

        for tick in ax.get_xticklabels():
            tick.set_rotation(45)
            tick.set_horizontalalignment("right")

        ax.xaxis.set_major_locator(dates.MinuteLocator(interval=interval))
        ax.xaxis.set_major_formatter(DateFormatter('%H:%M:%S'))
        ax.tick_params(which='major', labelsize=5)

        fig.suptitle(title, fontsize=8)
        plt.xlabel(x, fontsize=6)
        plt.ylabel(y, fontsize=6)
        title = title.replace(' ', '_')
        plt.savefig(output + "/" + title)

    @staticmethod
    def line_detail(df, fields, title, y, x='Date/Time', interval=60, output='output', diff=False):
        fig, ax = plt.subplots(1, 1, sharex='all', sharey='all')

        for i, field in enumerate(fields):
            grp = df.groupby(df.index)[field].sum().reset_index()
            grp.set_index(['timestamp'], inplace=True)
            if diff:
                grp[field] = grp[field].diff()
                grp = grp.apply(pd.Series.replace, to_replace=np.nan, value=0)
                grp[grp[field] < 0] = 0
                percentile = grp[field].quantile(0.9)
                grp[field].clip(0, percentile, inplace=True)
            ax.plot(grp.index, grp[field], label=field)

        for tick in ax.get_xticklabels():
            tick.set_rotation(45)
            tick.set_horizontalalignment("right")

        ax.xaxis.set_major_locator(dates.MinuteLocator(interval=interval))
        ax.xaxis.set_major_formatter(DateFormatter('%H:%M:%S'))
        ax.tick_params(which='major', labelsize=5)

        fig.suptitle(title, fontsize=8)
        plt.xlabel(x, fontsize=6)
        plt.ylabel(y, fontsize=6)
        plt.legend()
        title = title.replace(' ', '_')
        plt.savefig(output + "/" + title)

    @staticmethod
    def stack_summary(df, fields, title, y, x='Date/Time', interval=60, output='output', diff=False):
        fig, ax = plt.subplots(1, 1, sharex='all', sharey='none')

        df2 = pd.DataFrame()
        for i, field in enumerate(fields):
            grp = df.groupby(df.index)[field].sum().reset_index()
            grp.set_index(['timestamp'], inplace=True)
            if diff:
                grp[field] = grp[field].diff()
                grp = grp.apply(pd.Series.replace, to_replace=np.nan, value=0)
                grp[grp[field] < 0] = 0
                percentile = grp[field].quantile(0.9)
                grp[field].clip(0, percentile, inplace=True)
            df2[field] = grp[field]

        ax.stackplot(df2.index, df2.T, labels=fields)

        for tick in ax.get_xticklabels():
            tick.set_rotation(45)
            tick.set_horizontalalignment("right")

        ax.xaxis.set_major_locator(dates.MinuteLocator(interval=interval))
        ax.xaxis.set_major_formatter(DateFormatter('%H:%M:%S'))
        ax.tick_params(which='major', labelsize=5)

        fig.suptitle(title, fontsize=8)
        plt.xlabel(x, fontsize=6)
        plt.ylabel(y, fontsize=6)
        plt.legend()
        title = title.replace(' ', '_')
        plt.savefig(output + "/" + title)

    @staticmethod
    def broken_barh(df, fields, title, y, x='Date/Time', interval=60, output='output', barh_type='cpu'):
        index = df.index.drop_duplicates()
        resolution = (index[1:] - index[:-1]).value_counts()

        fig, ax = plt.subplots(1, 1)
        yaxis = 0
        ylabels = []
        legend_pool = []

        grp = df.groupby(fields[0])
        for key, item in grp:
            ylabels.append("{} {}".format(y, key))
            df2 = grp.get_group(key)
            df2 = df2.reset_index().\
                sort_values(['timestamp', fields[1]]).\
                drop_duplicates(['timestamp', fields[1], fields[0]], keep='first').\
                set_index(['timestamp'])
            df2 = df2[~df2.index.duplicated(keep='first')]

            prev_state = start_state = df2[fields[1]].iloc[0]
            prev_dt = start_dt = df2.first_valid_index()
            counter = 1
            seconds = 0
            for idx, row in df2.iterrows():
                counter += 1
                diff = idx - prev_dt
                seconds += diff.total_seconds()
                if diff > resolution.index[0] or prev_state != row[fields[1]]:
                    colour, label = MatplotlibGraphs.barh_labels(state=start_state, label_type=barh_type)
                    if label in legend_pool:
                        prefix = '_'
                    else:
                        legend_pool.append(label)
                        prefix = ''
                    ax.broken_barh(xranges=[(mdates.date2num(start_dt), seconds / 60 / 60 / 24)],
                                   yrange=(6 * (yaxis + 1), 5), facecolors=colour, label=prefix + label)
                    start_state = row[fields[1]]
                    start_dt = idx
                    counter = 0
                    seconds = 0
                prev_state = row[fields[1]]
                prev_dt = idx

            colour, label = MatplotlibGraphs.barh_labels(state=start_state, label_type=barh_type)
            if label in legend_pool:
                prefix = '_'
            else:
                legend_pool.append(label)
                prefix = ''
            ax.broken_barh(xranges=[(mdates.date2num(start_dt), seconds / 60 / 60 / 24)],
                           yrange=(6 * (yaxis + 1), 5), facecolors=colour, label=prefix + label)
            yaxis += 1

        ax.set_yticks([3 + 6 * x for x in range(1, yaxis + 1)])
        ax.set_yticklabels(ylabels)

        for tick in ax.get_xticklabels():
            tick.set_rotation(45)
            tick.set_horizontalalignment("right")

        ax.xaxis.set_major_locator(dates.MinuteLocator(interval=interval))
        ax.xaxis.set_major_formatter(DateFormatter('%H:%M:%S'))
        ax.tick_params(which='major', labelsize=5)

        fig.suptitle(title, fontsize=8)
        plt.xlabel(x, fontsize=6)
        plt.ylabel(y, fontsize=6)
        plt.legend()
        title = title.replace(' ', '_')
        plt.savefig(output + "/" + title)

    @staticmethod
    def barh_labels(state, label_type='cpu'):
        colour = 'white'
        label = 'Other'

        if label_type == 'cpu':
            if state == 'S':
                colour = 'lightblue'
                label = 'Sleep'
            elif state == 'Z':
                colour = 'black'
                label = 'Zombie'
            elif state == 'D':
                colour = 'darkgreen'
                label = 'Disk'
            elif state == 'R':
                colour = 'crimson'
                label = 'Running'
            else:
                colour = 'white'
                label = 'Other'

        return colour, label

    @staticmethod
    def bar_detail(data, title, y, x='Date/Time', output='output'):
        columns = np.arange(len(data['columns']))
        width = 0.4
        cmap = plt.cm.get_cmap('hsv', len(data))
        fig, ax = plt.subplots()
        i = 0

        for key, value in data.items():
            if key != 'columns':
                ax.bar(columns + (i * width), value, width=width, color=cmap(i + 1), align='center', label=key)
                i = i + 1

        plt.xlabel(x, fontsize=6)
        plt.ylabel(y, fontsize=6)
        ax.set_xticks(columns + width / 2)
        ax.set_xticklabels(data['columns'])
        plt.legend()
        title = title.replace(' ', '_')
        plt.savefig(output + "/" + title)
