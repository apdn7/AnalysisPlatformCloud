/**
 * @file Manages showing data of columns that cannot be converted to other data type.
 * @author Pham Minh Hoang <hoangpm6@fpt.com>
 */

/**
 * A class to manage show information of Error cast data type
 */
class FailedCastDataModal {
    /**
     * @type object
     * A dictionary that contains all function event of all elements
     */
    #Events;

    /**
     * A dictionary contains all element id of modal
     * @type {{columnsBox: string, btnOk: string, modal: string, alertFailedCaseDataErrorDiv: string, alertFailedCaseDataErrorMsg: string}}
     */
    static ElementIds = {
        modal: 'failedCastDataModal',
        btnOk: 'btnOKFailedCastDataModal',
        columnsBox: 'columnsBox',
        alertFailedCaseDataErrorDiv: 'alertFailedCaseDataErrorDiv',
        alertFailedCaseDataErrorMsg: 'alertFailedCaseDataErrorMsg',
    };

    constructor() {
        this.Elements = {};
        for (const [key, id] of Object.entries(
            FailedCastDataModal.ElementIds,
        )) {
            Object.defineProperty(this.Elements, key, {
                get: function () {
                    return document.getElementById(id);
                },
            });
        }

        this.#Events = {
            btnOk: function (event) {
                $(event.currentTarget)
                    .closest(`#${FailedCastDataModal.ElementIds.modal}`)
                    .modal('hide');
            },
        };
    }

    /**
     * Initialize modal and generate full layout of it
     * @param {object} data - a dictionary contains all columns & un-converted data
     * @param errorMessage - a string with error content
     */
    init = decoratorLog(function init(data, errorMessage = '') {
        const instance = this;
        instance.Elements.alertFailedCaseDataErrorMsg.textContent =
            errorMessage;
        instance.Elements.alertFailedCaseDataErrorDiv.style.display =
            errorMessage ? 'block' : 'none';
        instance.Elements.columnsBox.innerHTML = instance.#generateHTML(data);
        instance.#injectEvents();
    }, this.constructor.name);

    /**
     * Show modal
     * @function
     */
    show = decoratorLog(function show() {
        const instance = this;
        $(instance.Elements.modal).modal('show');
    }, this.constructor.name);

    /**
     * Generate a HTML that contains all columns & un-converted data
     * @param {object} data
     * @return {string} a string HTML that contains all columns & un-converted data
     * @private
     */
    #generateHTML = decoratorLog(function generateHTML(data) {
        const instance = this;
        let html = '';
        for (const [column_id, dataDict] of Object.entries(data)) {
            const column = dataDict.detail;
            const dataList = dataDict.data;
            html += `<div class="d-flex flex-column filter-data">
\t<div class="column-name">
\t\t<span class="d-inline-block" title="${column.shown_name}">${column.shown_name}</span>
\t</div>
\t<div id="div_failed_cast_value_${column_id}" class="column-datas active">`;

            let valuesHTML = '';
            dataList.forEach(
                (value) => (valuesHTML += instance.#generateValueHTML(value)),
            );

            html += valuesHTML;
            html += `\t</div>
</div>`;
        }

        return html;
    }, this.constructor.name);

    /**
     * Generate html that contain input value
     * @param {string} value - A string value
     * @return {string} a string HTML
     * @private
     */
    #generateValueHTML = decoratorLog(
        function generateValueHTML(value) {
            return `\t\t<div class="p-1 list-group-item">
\t\t\t<span class="custom-control-label">${value}</span>
\t\t</div>`;
        },
        this.constructor.name,
        false,
    );

    /**
     * Add event to related elements on modal
     * @function
     * @private
     */
    #injectEvents = decoratorLog(function injectEvents() {
        const instance = this;
        instance.Elements.btnOk.removeEventListener(
            'click',
            instance.#Events.btnOk,
            false,
        );
        instance.Elements.btnOk.addEventListener(
            'click',
            instance.#Events.btnOk,
        );
    }, this.constructor.name);
}
