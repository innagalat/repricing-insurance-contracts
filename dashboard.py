import pandas as pd
import numpy as np
import os

import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go

df = pd.read_csv('C:\\galati_files\\pyscripts\\callo-repricing\\compare-runs\\results\\ordinary-cover\\final\\dataset-dash.csv')

print df.info()


id_options = df['L_LIFE_ID'].unique()

app = dash.Dash()

app.layout = html.Div([
	html.H2("Report XX"),
    html.Div(
        [
            dcc.Dropdown(
                id="Life",
                options=[{
                    'label': i,
                    'value': i
                } for i in id_options],
                value='All Lives'),
        ],
        style={'width': '25%',
               'display': 'inline-block'}),
    dcc.Graph(id='funnel-graph'),
    dcc.Tabs(
        tabs=[
            {'label': 'Tab {}'.format(i), 'value': i} for i in range(1, 5)
        ],
        value=3,
        id='tabs'
    ),
])


@app.callback(
    dash.dependencies.Output('funnel-graph', 'figure'),
    [dash.dependencies.Input('Life', 'value')])


def update_graph(Life):
    if Life == 'All Lives':
        df_plot = df.copy()
    else:
        df_plot = df[df['L_LIFE_ID'] == Life]

    pv = pd.pivot_table(
        df_plot,
        index=['Benefit'],
        columns=["Status"],
        values=['Values'],
        aggfunc=sum,
        fill_value=0)

    trace1 = go.Bar(x=pv.index, y=pv[('Values', 'PREM_1')], name='PREM_1')
    trace2 = go.Bar(x=pv.index, y=pv[('Values', 'PREM_13')], name='PREM_13')
    # trace3 = go.Bar(x=pv.index, y=pv[('Values', 'presented')], name='Presented')
    # trace4 = go.Bar(x=pv.index, y=pv[('Values', 'won')], name='Won')

    return {
        'data': [trace1, trace2],  #, trace3, trace4],
        'layout':
        go.Layout(
            title='Premium Values for {}'.format(Life),
            barmode='stack')
    }


if __name__ == '__main__':
    app.run_server(debug=True)
