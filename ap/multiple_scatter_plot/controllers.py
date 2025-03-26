import os

from flask import Blueprint

from ap.common.flask_customize import render_template
from ap.common.services.form_env import get_common_config_data

multiple_scatter_plot_blueprint = Blueprint(
    'multiple_scatter_plot',
    __name__,
    template_folder=os.path.join('..', 'templates', 'multiple_scatter_plot'),
    static_folder=os.path.join('..', 'static', 'multiple_scatter_plot'),
    # static_url_path='../static/trace_data',
    url_prefix='/ap',
)


@multiple_scatter_plot_blueprint.route('/msp')
def index():
    output_dict = get_common_config_data()
    return render_template('multiple_scatter_plot.html', **output_dict)
