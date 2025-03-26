import os

from flask import Blueprint

from ap.common.flask_customize import render_template
from ap.common.services.form_env import get_common_config_data

sankey_plot_blueprint = Blueprint(
    'sankey_plot',
    __name__,
    template_folder=os.path.join('..', 'templates', 'sankey_plot'),
    static_folder=os.path.join('..', 'static', 'sankey_plot'),
    url_prefix='/ap',
)


@sankey_plot_blueprint.route('/skd')
def index():
    output_dict = get_common_config_data()
    return render_template('sankey_plot.html', **output_dict)
