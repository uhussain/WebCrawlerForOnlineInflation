import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

import pandas as pd
import plotly.graph_objs as go

# Step 1. Launch the application
app = dash.Dash()

# Step 2. Import the dataset
filepath = 'datasets/laptop_trends_2017-2020.csv'
st = pd.read_csv(filepath)

# dropdown options
features = st.columns[2:-1]
opts = [{'label' : i, 'value' : i} for i in features]

# range slider options
#st['Year'] = pd.to_datetime(st.Year)
#dates = ['2015-02-17', '2015-05-17', '2015-08-17', '2015-11-17',
#         '2016-02-17', '2016-05-17', '2016-08-17', '2016-11-17', '2017-02-17']

dates = [2017,2018,2019,2020]

# Step 3. Create a plotly figure
trace_1 = go.Scatter(x = st.Year, y = st['Low-Level'],
                    name = 'Affordable',
                    line = dict(width = 2,
                                color = 'rgb(229, 151, 50)'))
layout = go.Layout(title = 'Tracking Laptop Prices',
                    xaxis={'title': 'Year'},
                    yaxis={'title': 'Avg. Online Price'},
                    margin={'l': 40, 'b': 40, 't': 10, 'r': 10},
                    legend={'x': 0, 'y': 1},
                    hovermode = 'closest')
fig = go.Figure(data = [trace_1], layout = layout)


# Step 4. Create a Dash layout
app.layout = html.Div([
                # a header and a paragraph
                html.Div([
                    html.H1("Price Crawler"),
                    html.P("Tracking Price Inflation is so interesting!!")
                         ],
                     style = {'padding' : '50px' ,
                              'backgroundColor' : '#3aaab2'}),
                # adding a plot
                dcc.Graph(id = 'plot', figure = fig),
                # dropdown
                html.P([
                    html.Label("Choose type of laptop"),
                    dcc.Dropdown(id = 'opt', options = opts,
                                value = opts[0])
                        ], style = {'width': '400px',
                                    'fontSize' : '20px',
                                    'padding-left' : '100px',
                                    'display': 'inline-block'}),
                # range slider
                html.P([
                    html.Label("Time Period"),
                    dcc.RangeSlider(id = 'slider',
                                    marks = {i : dates[i] for i in range(0, 4)},
                                    min = 0,
                                    max = 3,
                                    value = [0, 3])
                        ], style = {'width' : '80%',
                                    'fontSize' : '20px',
                                    'padding-left' : '100px',
                                    'display': 'inline-block'})
                      ])


# Step 5. Add callback functions
@app.callback(Output('plot', 'figure'),
             [Input('opt', 'value'),
             Input('slider', 'value')])
def update_figure(input1, input2):
    # filtering the data
    st2 = st[(st.Year >= dates[input2[0]]) & (st.Year <= dates[input2[1]])]
    # updating the plot
    trace_1 = go.Scatter(x = st2.Year, y = st2['Low-Level'],
                        name = 'Affordable',
                        line = dict(width = 2,
                                    color = 'rgb(229, 151, 50)'))
    trace_2 = go.Scatter(x = st2.Year, y = st2[input1],
                        name = input1,
                        line = dict(width = 2,
                                    color = 'rgb(106, 181, 135)'))
    fig = go.Figure(data = [trace_1, trace_2], layout = layout)
    return fig
  
# Step 6. Add the server clause
if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=8000)
