import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import warnings

warnings.filterwarnings(action="once")

large = 22
med = 16
small = 12
params = {
    "legend.fontsize": med,
    "figure.figsize": (16, 10),
    "axes.labelsize": med,
    "axes.titlesize": med,
    "xtick.labelsize": med,
    "ytick.labelsize": med,
    "figure.titlesize": large,
}
plt.rcParams.update(params)
plt.style.use("seaborn-whitegrid")
sns.set_style("white")

__all__ = ["scatterplot_with_hist"]


def scatterplot_with_hist(
    data: pd.DataFrame,
    x_var: str = None,
    y_var: str = None,
    group_var: str = None,
    n_x_bins: int = 20,
    n_y_bins: int = 20,
    weights: str = None,
    title: str = None,
    group_hist: bool = False,
    figsize: tuple = (16, 10),
    dpi: int = 80,
) -> None:
    """
    Generate a scatterplot and histograms for x and y variables from a DataFrame

    Parameters
    ----------
    data : pd.DataFrame
        Location of template file
    x_var: str optional
        Name of variable for x-axis
    y_var: str optional
        Name of variable for y-axis
    group_var: str optional
        Name of variable to colour scatterplot by
    n_x_bins: int optional
        Number of bins for x-axis historgram
    n_y_bins: int optional
        Number of bins for y-axis historgram
    weights: str optional
        Weights for each historgram
    title: str optional
        Title of plot
    group_hist: bool optional
        Boolean flag to plot histograms as stacked barcharts using group_var
    figsize: tuple optional
        Figure size of plot
    dpi: int = optional
        Dots Per Inch resolution of plot

    Returns
    -------
    None
    """

    # check input Dataframe is at least 2 columns
    try:
        # get columns to plot
        if x_var is None:
            x_var = data.columns[0]

        if y_var is None:
            y_var = data.columns[1]
    except IndexError as e:
        print("Error: {0}\n\tNeed at least 2 columns in input Dataframe\n".format(e))
        return None

    # group and colour plots based on group_var
    colour = None
    if group_var is not None:
        color_labels = data[group_var].unique()
        # List of RGB triplets
        rgb_values = sns.color_palette("Set2", len(color_labels))
        # Map label to RGB
        color_map = dict(zip(color_labels, rgb_values))
        colour = data[group_var].map(color_map)

    # work out title
    if title is None:
        title = "{0:s} vs {1:s}".format(y_var, x_var)

    # Create Fig and gridspec
    fig = plt.figure(figsize=figsize, dpi=dpi)
    grid = plt.GridSpec(4, 4, hspace=0.5, wspace=0.2)

    # Define the axes
    ax_main = fig.add_subplot(grid[:-1, :-1])
    ax_right = fig.add_subplot(grid[:-1, -1], xticklabels=[], yticklabels=[])
    ax_bottom = fig.add_subplot(grid[-1, 0:-1], xticklabels=[], yticklabels=[])

    # Scatterplot on main ax
    ax_main.scatter(
        x_var,
        y_var,
        data=data,
        c=colour,
        alpha=0.9,
        cmap="tab10",
        edgecolors="gray",
        linewidths=0.5,
    )

    # histogram on the bottom
    if group_var is None or group_hist is False:
        df_hist_x = data[x_var]
    else:
        df_agg = data.loc[:, [x_var, group_var]].groupby(group_var)
        df_hist_x = [data[x_var].values.tolist() for i, df in df_agg]

    ax_bottom.hist(df_hist_x, n_x_bins, histtype="barstacked", orientation="vertical")
    ax_bottom.invert_yaxis()

    # histogram on the right
    if group_var is None or group_hist is False:
        df_hist_y = data[y_var]
    else:
        df_agg = data.loc[:, [y_var, group_var]].groupby(group_var)
        df_hist_y = [data[y_var].values.tolist() for i, df in df_agg]

    ax_right.hist(df_hist_y, n_y_bins, histtype="barstacked", orientation="horizontal")

    # Decorations
    ax_main.set(title=title, xlabel=x_var, ylabel=y_var)
    ax_main.title.set_fontsize(20)
    for item in (
        [ax_main.xaxis.label, ax_main.yaxis.label]
        + ax_main.get_xticklabels()
        + ax_main.get_yticklabels()
    ):
        item.set_fontsize(14)

    if not isinstance(ax_main.get_xticks(), list):
        xlabels = ax_main.get_xticks().tolist()
        ax_main.set_xticklabels(xlabels)

    plt.show()
