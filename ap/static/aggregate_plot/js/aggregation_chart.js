const COLOR_DEFAULT = [
    '#1f77b4',
    '#ff7f0e',
    '#2ca02c',
    '#d62728',
    '#9467bd',
    '#8c564b',
    '#e377c2',
    '#7f7f7f',
    '#bcbd22',
];
const AGG_FUNCTION_WITHOUT_CALCULATED_VALUE = ['max', 'min'];

const drawAgPPlot = (
    data,
    plotData,
    countByXAxis,
    div,
    isCyclicCalender,
    canvasId,
    yScale,
    yAxisDisplayMode,
    divFromTo,
) => {
    const {
        agg_function,
        color_name,
        unique_color,
        fmt,
        end_col_format,
        color_col_format,
        div_col_format,
        shown_name,
    } = plotData;

    const isLineChart = agg_function && agg_function.toLowerCase() !== 'count';
    let showPercent =
        [
            AGP_YAXIS_DISPLAY_MODES.Y_AXIS_TOTAL,
            AGP_YAXIS_DISPLAY_MODES.Y_AXIS_FACET,
        ].includes(yAxisDisplayMode) && !isLineChart;

    const yAxisTickFormat =
        isLineChart && end_col_format
            ? end_col_format
            : fmt
              ? fmt.includes('e')
                  ? '.1e'
                  : fmt
              : '';
    const yAxisTickFormatPercent = showPercent
        ? ',.0%'
        : fmt
          ? fmt.includes('e')
              ? '.1e'
              : fmt
          : '';
    const shouldReformatValueOnHover =
        isLineChart &&
        AGG_FUNCTION_WITHOUT_CALCULATED_VALUE.includes(
            agg_function.toLowerCase(),
        );
    const shouldMarkValueAsHyphen =
        isLineChart && percentagePattern.test(end_col_format);

    showPercent = showPercent && !isLineChart;
    let xTitles = data[0] ? [...data[0].x] : [];
    xTextFormat = xTitles.map((x) => x.slice(1));
    if (div_col_format !== null && div_col_format !== '') {
        xTextFormat = xTextFormat.map((x) => d3.format(div_col_format)(x));
    }

    const tickLen = xTitles.length ? xTitles[0].length : 0;
    const tickSize = tickLen > 5 ? 10 : 12;

    data = prepareColorForTrace(data, unique_color);
    if (isLineChart && yScale) {
        data = getOutlierTraceData(data, yScale, plotData);
    }

    data = prepareColorFormat(data, color_col_format);

    let yMin, yMax;
    if (isLineChart) {
        const offset = (yScale['y-max'] - yScale['y-min']) * 0.018;
        yMin = yScale['y-min'] - offset;
        yMax = yScale['y-max'] + offset;
    }

    const layout = {
        barmode: 'stack',
        plot_bgcolor: '#222222',
        paper_bgcolor: '#222222',
        autosize: true,
        xaxis: {
            tickmode: 'array',
            ticktext: reduceTicksArray(xTextFormat, tickLen),
            tickvals: reduceTicksArray(xTitles, tickLen),
            gridcolor: '#444444',
            tickfont: {
                color: 'rgba(255,255,255,1)',
                size: tickSize,
            },
            spikemode: 'across',
            spikethickness: 1,
            spikedash: 'solid',
            spikecolor: 'rgb(255, 0, 0)',
            tickformat: 'c',
            domain: [0, 1],
            nticks: 8,
        },
        yaxis: {
            gridcolor: '#444444',
            tickfont: {
                color: 'rgba(255,255,255,1)',
                size: 12,
            },
            spikemode: 'across',
            spikethickness: 1,
            spikedash: 'solid',
            spikecolor: 'rgb(255, 0, 0)',
            tickformat: yAxisTickFormat,
            range: showPercent ? [0, 1] : yScale ? [yMin, yMax] : null,
            autorange: yScale ? false : true,
        },
        showlegend: true,
        legend: {
            title: {
                text: `${showPercent ? '%' : agg_function}<br><sub>${color_name || shown_name}</sub>`,
            },
            font: {
                family: 'sans-serif',
                size: 12,
                color: '#ffffff',
            },
            bgcolor: 'transparent',
            xanchor: 'right',
            x: 1.07,
            // itemsizing: "constant",
            // itemwidth: 200
        },
        margin: {
            b: 60,
            t: 20,
            r: 10,
        },
    };

    if (showPercent) {
        layout.yaxis.ticksuffix = '%';
        data.forEach((item) => {
            item.y = item.y.map((i) => i * 100);
        });
    }

    if (isLineChart) {
        layout.xaxis.range = [-0.5, div.length - 0.5];
        layout.legend.traceorder = 'reversed';
    }

    const heatmapIconSettings = genPlotlyIconSettings();
    const config = {
        ...heatmapIconSettings,
        responsive: true, // responsive histogram
        useResizeHandler: true, // responsive histogram
        style: { width: '100%', height: '100%' },
    };
    Plotly.react(canvasId, data, layout, config);

    const agPPlot = document.getElementById(canvasId);

    agPPlot.on('plotly_hover', (data) => {
        const dpIndex = getDataPointIndex(data);
        const {
            x,
            y,
            name,
            type,
            isOutlier,
            colorName,
            outlierVal,
            colId,
            div_master_ids,
            div_master_column_name,
            color_master_id,
            color_master_col_name,
        } = data.points[0].data;
        const xVal = x[dpIndex].slice(1);
        const divMasterId = div_master_ids[dpIndex] ?? null;
        const masterInfoDiv = getMasterInfoHovering(
            divMasterId,
            div_master_column_name,
        );
        const colorMasterId =
            typeof color_master_id === 'object' ? null : color_master_id;
        const masterInfoColor = getMasterInfoHovering(
            colorMasterId,
            color_master_col_name,
        );
        const color = colorName || name;
        const hasColor = !!color_name;
        const nByXAndColor = y[dpIndex];
        let dataTable = '';
        const isShowFromTo = div.length === useDivFromTo.length - 1;
        const period = [];
        const fromTo = [];
        if (isCyclicCalender && isShowFromTo) {
            const index = div.indexOf(xVal);
            let from, to;
            if (index !== -1) {
                from = useDivFromTo[index];
                to = useDivFromTo[index + 1];
            }

            if (from && to) {
                period.push([
                    'Period',
                    `${from}${DATETIME_PICKER_SEPARATOR}${to}`,
                ]);
            }
        }

        if (divFromTo) {
            // show from, to of Data number division
            const divIndex = div.indexOf(xVal);
            if (divIndex !== -1) {
                const fromToOb = divFromTo[divIndex];
                fromTo.push(
                    ['From', formatDateTime(fromToOb[0])],
                    ['To', formatDateTime(fromToOb[1])],
                );
            }
        }
        if (type.includes('lines') || isOutlier) {
            const showVal = [];
            const functionName = isOutlier ? i18n.outlier : agg_function;
            let value = isOutlier ? outlierVal[dpIndex] : nByXAndColor;
            if (shouldReformatValueOnHover) {
                value = applySignificantDigit(
                    value,
                    undefined,
                    yAxisTickFormat,
                );
            } else if (shouldMarkValueAsHyphen) {
                value = '-';
            } else {
                value = applySignificantDigit(value);
            }
            showVal.push([functionName, value]);
            dataTable = genHoverDataTable(
                [
                    ['x', xVal],
                    ...period,
                    ['Color', color],
                    ...showVal,
                    ...fromTo,
                ],
                masterInfoDiv,
                masterInfoColor,
            );
        } else {
            const nByX = showPercent ? '100%' : countByXAxis[colId][xVal];
            const NByColor = showPercent
                ? `${applySignificantDigit(nByXAndColor)}%`
                : applySignificantDigit(nByXAndColor);

            const NByColorHover = hasColor
                ? [['N by x and Color', NByColor]]
                : [];

            dataTable = genHoverDataTable(
                [
                    ['x', xVal],
                    ...period,
                    ['Color', color],
                    ...NByColorHover,
                    ['N by x', applySignificantDigit(nByX)],
                    ...fromTo,
                ],
                masterInfoDiv,
                masterInfoColor,
            );
        }
        genDataPointHoverTable(
            dataTable,
            {
                x: data.event.pageX - 220,
                y: data.event.pageY,
            },
            0,
            true,
            canvasId,
            1,
        );
    });
    unHoverHandler(agPPlot);
};

const reduceTicksArray = (array, tickLen) => {
    const MAX_TICKS = 9;
    const nTicks = MAX_TICKS;
    const isReduce = array.length > MAX_TICKS;
    if (!isReduce) return array;
    let nextIndex =
        array.length / nTicks < 2 ? 2 : Math.round(array.length / nTicks);
    if (nextIndex * nTicks > MAX_TICKS) {
        nextIndex += 1;
    }
    const res = [];
    let i = 0;
    while (i < array.length) {
        res.push(array[i]);
        i += nextIndex;
    }

    return res;
};

const prepareColorFormat = (data, colorColFormat) => {
    if (colorColFormat === null || colorColFormat === '') {
        return data;
    }
    const f = d3.format(colorColFormat);
    return data.map((da) => ({
        ...da,
        name: da.name === 'Other' ? da.name : f(da.name),
    }));
};

const prepareColorForTrace = (data, uniqueColor) => {
    let styles = [];
    if (uniqueColor.length > 0) {
        styles = uniqueColor.map((color, k) => ({
            target: color,
            color: COLOR_DEFAULT[k],
        }));
    } else {
        styles = data.map((data, k) => ({
            target: data.name,
            color: COLOR_DEFAULT[k],
        }));
    }

    return data.map((da) => {
        const colors = styles.filter((st) => st.target === da.name);
        const color =
            colors.length > 0
                ? styles.filter((st) => st.target === da.name)[0].color
                : '';
        return {
            ...da,
            marker: {
                color: color,
            },
            line: {
                ...da.line,
                color: color,
            },
        };
    });
};

const getOutlierTraceData = (datas, yScale, plotData) => {
    const { lower_outlier_idxs, upper_outlier_idxs } = yScale;
    const { array_y } = plotData;

    const outlierTraceList = [];

    datas = datas.map((data) => {
        let isHasOutlier = false;
        const outlierTrace = {
            ...data,
            colorName: data.name,
            name: i18n.outlier,
            mode: 'markers',
            marker: {
                symbol: '4',
                size: 8,
            },
            isOutlier: true,
            showlegend: false,
        };
        let cloneY = Array.from(outlierTrace.y).fill(null);
        let outlierVal = Array.from(outlierTrace.y).fill(null);
        for (const i of lower_outlier_idxs) {
            const lowerOutlier = array_y[i];
            const indexList = getAllIndexes(data.y, lowerOutlier);
            for (const index of indexList) {
                isHasOutlier = true;
                cloneY[index] = yScale['y-min'];
                outlierVal[index] = data.y[index];
                data.y[index] = null;
            }
        }

        for (const i of upper_outlier_idxs) {
            const upperOutlier = array_y[i];
            const indexList = getAllIndexes(data.y, upperOutlier);
            for (const index of indexList) {
                isHasOutlier = true;
                cloneY[index] = yScale['y-max'];
                outlierVal[index] = data.y[index];
                data.y[index] = null;
            }
        }
        if (isHasOutlier) {
            outlierTrace.y = cloneY;
            outlierTrace.outlierVal = outlierVal;
            outlierTraceList.push(outlierTrace);
        }

        return data;
    });

    datas = outlierTraceList.concat(datas);

    return datas;
};

function getAllIndexes(arr, val) {
    let indexes = [],
        i = -1;
    while ((i = arr.indexOf(val, i + 1)) != -1) {
        indexes.push(i);
    }
    return indexes;
}
