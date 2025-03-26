const buildTimeseriesSummaryResultsHTML = (
    summaryOption,
    tableIndex,
    generalInfo,
    beforeRankValues = null,
    stepChartSummary = null,
    isCTCol = false,
    unit = null,
) => {
    const { getProc } = generalInfo;
    const { getVal } = generalInfo;
    const { catExpBox } = generalInfo;
    const { facetFormat } = generalInfo;
    const { endProcId } = generalInfo;
    const { sensorId } = generalInfo;
    const [nTotalHTML, noLinkedHTML] = genTotalAndNonLinkedHTML(
        summaryOption,
        generalInfo,
    );
    let catExpBoxHtml = '';
    // let unit = {};
    // _.pickBy(procUnitJsons[endProcId], function (data_group) {
    //     const isMapped = data_group.data_name_jp === getVal;
    //     if (isMapped) unit = data_group.unit;
    //     return isMapped;
    // });
    if (unit && unit !== '' && unit !== 'Null') {
        unit = ` [${unit}]`;
    } else {
        unit = '';
    }

    let CTLabel = '';
    if (isCTCol) {
        CTLabel = `(${DataTypes.DATETIME.short}) [sec]`;
    }

    if (catExpBox || catExpBox === '') {
        const hasLevel2 = catExpBox.toString().split('|').length === 2;
        let catExpBoxLabel = catExpBox === '' ? COMMON_CONSTANT.NA : catExpBox;
        catExpBoxLabel = formatCatExpBox(catExpBoxLabel, facetFormat);

        catExpBoxHtml = `
        <tr>
            <td colspan="2">
                <span class="prc-name show-detail cat-exp-box" title="${hasLevel2 ? 'Level1 | Level2' : 'Level1'}">${catExpBoxLabel}</span>
            </td>
        </tr>`;
    }

    const tableTitle = `
        <thead>
        <tr>
            <td colspan="2">
                <span class="prc-name" title="${getProc}">${getProc}</span>
            </td>
        </tr>
        <tr>
            <td colspan="2">
                <span class="prc-name" title="${getVal}${unit}">${getVal}${unit} ${CTLabel}</span>
            </td>
        </tr>
        ${catExpBoxHtml}
        </thead>
    `;

    if (beforeRankValues) {
        let stepChartStatHTML = '';
        let stepChartNTotalHTML = `<tr>
            <td><span class="hint-text" title="${i18nCommon.hoverNTotal}">N<sub>total</sub></span></td>
            <td>
                ${nTotalHTML}
            </td>
        </tr>`;
        if (stepChartSummary) {
            let stepChartStatUnLinkedHTML = '';
            const naAfterLinked =
                stepChartSummary.n_na - summaryOption.countUnlinked;
            if (`${generalInfo.endProcName}` !== `${generalInfo.startProc}`) {
                stepChartStatUnLinkedHTML = `<tr>
                    <td><span class="">N<sub>NoLinked</sub></span></td>
                    <td>${applySignificantDigit(summaryOption.countUnlinked)} (${summaryOption.noLinkedPct}%)</td>
                </tr>`;
            }
            const naAfterLinkedPctg =
                stepChartSummary.n_na_pctg - summaryOption.noLinkedPct;
            stepChartNTotalHTML = `<tr>
                    <td><span class="hint-text" title="${i18nCommon.hoverNTotal}">N<sub>total</sub></span></td>
                    <td>
                        ${isEmpty(stepChartSummary.n_total) ? '-' : `${applySignificantDigit(stepChartSummary.n_total)} (100%)`}
                    </td>
                </tr>`;
            stepChartStatHTML = `<tr>
                    <td><span class="item-name">N</span></td>
                    <td>
                        ${isEmpty(stepChartSummary.n) ? '-' : `${applySignificantDigit(stepChartSummary.n)} (${stepChartSummary.n_pctg}%)`}
                    </td>
                </tr>
                <tr>
                    <td><span class="item-name">N<sub>NA</sub></span></td>
                    <td>
                        ${isEmpty(stepChartSummary.n_na) ? '-' : `${applySignificantDigit(naAfterLinked)} (${naAfterLinkedPctg}%)`}
                    </td>
                </tr>
                ${stepChartStatUnLinkedHTML}`;
        }
        return `
        <table class="result count" style="margin-top: 10px;">
            ${tableTitle}
            <tbody>
                ${stepChartNTotalHTML}
                ${stepChartStatHTML}
            </tbody>
        </table>
        <table class="result basic-statistics" style="margin-top: 10px;">
            ${tableTitle}
            <tbody>
                <tr>
                    <td><span class="item-name hint-text" title="${i18nCommon.hoverN}">N</span></td>
                    <td>
                        ${isEmpty(summaryOption.nStats) ? '-' : applySignificantDigit(summaryOption.nStats)}
                    </td>
                </tr>
            </tbody>
        </table>
        <table class="result non-parametric" style="margin-top: 10px;">
            ${tableTitle}
        </table>
        `;
    }

    const summaryHtml = buildSummaryResultsHTML(
        summaryOption,
        tableIndex,
        generalInfo,
        beforeRankValues,
        stepChartSummary,
    );
    return `
        <div style="width: 100%">
            <table style="width: 100%; margin-top: 10px">
            ${tableTitle}
            </table>
            ${summaryHtml}
        </div>
    `;
};

const removeClass = (element) => {
    const colClasses = element
        .prop('className')
        .split(' ')
        .filter((x) => x.startsWith('col-sm'));
    for (const cls of colClasses) {
        element.removeClass(cls);
    }
};

const onChangeSummaryEventHandler = (showScatterPlot) => {
    $('input[name=summaryOption]').on('change', function f() {
        const summaryClass = $(this).val();

        if (summaryClass === 'none') {
            $('.time-series').each(function changeColWidth() {
                removeClass($(this));
                if (!showScatterPlot) {
                    $(this).addClass('col-sm-9');
                } else {
                    $(this).addClass('col-sm-8');
                }
            });

            $('.summary-col').each(function showHideSummary() {
                $(this).removeClass('col-sm-2');
                $(this).css('display', 'none');
            });
            $('.ts-col').each(function changeTSChartWidth() {
                $(this).removeClass('col-sm-10');
                $(this).addClass('col-sm-12');
            });

            $('.tschart-title-parent').show();
        } else {
            $('.time-series').each(function changeColWidth() {
                removeClass($(this));
                if (!showScatterPlot) {
                    $(this).addClass('col-sm-9');
                } else {
                    $(this).addClass('col-sm-8');
                }
            });
            $('.ts-col').each(function changeTSChartWidth() {
                $(this).removeClass('col-sm-12');
                $(this).addClass('time-series-col');
            });
            $('.summary-col').each(function showHideSummary() {
                $(this).css('display', 'block');
            });
            $('.result').each(function showUponOption() {
                $(this).css('display', 'none');
                if ($(this).hasClass(summaryClass)) {
                    $(this).css('display', 'block');
                }
            });
            $('.tschart-title-parent').hide();
        }
        // adjust cate-table length
        // get width of current Time Series chart
        setTimeout(() => {
            adjustCatetoryTableLength();
            // Reposition cross line of label plot
            resetPositionOfCrossLine();
        }, 500);

        // histogram tab, summary select menu
        onChangeHistSummaryEventHandler(this);
    });
};

const onChangeHistSummaryEventHandler = (e) => {
    let summaryHeight = null;
    const summaryClass = $(e).val();
    const previousOption = $('input[name=summaryOption][data-checked=true]');
    if (summaryClass === 'none') {
        $('.hist-summary').each(function showHideSummary() {
            $(this).css('display', 'none');
        });

        if (previousOption.val() && previousOption.val() !== 'none') {
            // rescale histogram
            $('.his .hd-plot').each(function reScaleHistogram() {
                const histogramId = $(this).attr('id');
                $(`#${histogramId}`).css('height', GRAPH_CONST.histHeight);
                Plotly.relayout(histogramId, {});
            });
        }

        // mark this option as checked and remove others
        $(e).attr('data-checked', 'true');
        $('input[name=summaryOption]:not(:checked)').removeAttr('data-checked');
    } else {
        $('.hist-summary').each(function showHideSummary() {
            $(this).css('display', 'flex');
            $(this).css('justify-content', 'center');
        });
        $('.hist-summary-detail').each(function showUponOption() {
            $(this).css('display', 'none');
            if ($(this).hasClass(summaryClass)) {
                $(this).css('display', 'block');
                const h = $(this).height();
                summaryHeight = h < summaryHeight ? summaryHeight : h;
            }
        });

        // rescale only when from none -> not-none or not-none -> none to improve performance
        $('.his .hd-plot').each(function reScaleHistogram() {
            const histogramId = $(this).attr('id');
            const chartHeight = `calc(100% - ${summaryHeight + 6}px)`;
            $(`#${histogramId}`).css('height', chartHeight);
            Plotly.relayout(histogramId, {});
        });

        // mark this option as checked and remove others
        $(e).attr('data-checked', 'true');
        $('input[name=summaryOption]:not(:checked)').removeAttr('data-checked');
    }
};
