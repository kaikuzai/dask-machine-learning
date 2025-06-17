import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import dash
from dash import dcc, html, Input, Output, callback
import plotly.express as px
import plotly.graph_objects as go
from pymongo import MongoClient
from collections import Counter
import pandas as pd

from database.mongo_storage import MongoDatabaseClient

client = MongoDatabaseClient()
collection = client.init_mongodb_connection()

# Initialize Dash app
app = dash.Dash(__name__)
app.title = "Hotel Reviews Dashboard"

# Define the layout
app.layout = html.Div([
    html.Div([
        html.H1("Hotel Reviews Dashboard", 
                style={'textAlign': 'center', 'marginBottom': 20, 'color': '#2c3e50', 'fontSize': '2.5rem'}),
        html.P("Filter and explore hotel reviews stored in MongoDB.",
               style={'textAlign': 'center', 'marginBottom': 40, 'color': '#7f8c8d', 'fontSize': '1.1rem'})
    ], style={'padding': '20px 0'}),
    
    # Main content container
    html.Div([
        # Filters Section
        html.Div([
            html.H3("Filters", style={'marginBottom': 25, 'color': '#2c3e50'}),
            
            html.Div([
                html.Label("Hotel Name:", style={'fontWeight': 'bold', 'marginBottom': 8, 'display': 'block'}),
                dcc.Input(
                    id='hotel-name-input',
                    type='text',
                    placeholder='Enter hotel name...',
                    style={'width': '100%', 'padding': '10px', 'marginBottom': 20, 'border': '1px solid #ddd', 'borderRadius': '4px'}
                )
            ]),
            
            html.Div([
                html.Label("Nationality:", style={'fontWeight': 'bold', 'marginBottom': 8, 'display': 'block'}),
                dcc.Input(
                    id='nationality-input',
                    type='text',
                    placeholder='Enter nationality...',
                    style={'width': '100%', 'padding': '10px', 'marginBottom': 20, 'border': '1px solid #ddd', 'borderRadius': '4px'}
                )
            ]),
            
            html.Div([
                html.Label("Score Range:", style={'fontWeight': 'bold', 'marginBottom': 8, 'display': 'block'}),
                dcc.RangeSlider(
                    id='score-range-slider',
                    min=0,
                    max=10,
                    step=0.1,
                    value=[0, 10],
                    marks={i: str(i) for i in range(0, 11)},
                    tooltip={"placement": "bottom", "always_visible": True}
                )
            ], style={'marginBottom': 20})
        ], style={
            'width': '300px', 
            'padding': '25px',
            'backgroundColor': '#f8f9fa',
            'borderRadius': '8px',
            'marginRight': '25px',
            'height': 'fit-content'
        }),
        
        # Charts and data section
        html.Div([
            # Metrics row
            html.Div(id='metrics-cards', style={'marginBottom': 25}),
            
            # Charts row
            html.Div([
                html.Div([
                    dcc.Graph(id='score-distribution-chart')
                ], style={'width': '48%', 'display': 'inline-block', 'marginRight': '4%'}),
                
                html.Div([
                    dcc.Graph(id='top-hotels-chart')
                ], style={'width': '48%', 'display': 'inline-block'})
            ]),
            
            html.Div([
                dcc.Graph(id='top-nationalities-chart')
            ], style={'marginTop': 20}),
            
            # Data Table
            html.Div(id='data-table', style={'marginTop': 25})
            
        ], style={'flex': '1'})
        
    ], style={'display': 'flex', 'maxWidth': '1200px', 'margin': '0 auto', 'padding': '0 20px'})
])

def query_mongodb(hotel_name=None, nationality=None, min_score=0, max_score=10):
    """Query MongoDB directly without converting to DataFrame"""
    query = {}
    
    if hotel_name:
        query['Hotel_Name'] = {'$regex': hotel_name, '$options': 'i'}
    if nationality:
        query['Reviewer_Nationality'] = {'$regex': nationality, '$options': 'i'}
    
    # Score range filter
    if min_score > 0 or max_score < 10:
        query['Reviewer_Score'] = {'$gte': min_score, '$lte': max_score}
    
    return list(collection.find(query))

def calculate_metrics(data):
    """Calculate metrics directly from MongoDB data"""
    if not data:
        return {
            'total_reviews': 0,
            'avg_score': 0,
            'most_common_nationality': 'N/A',
            'most_reviewed_hotel': 'N/A'
        }
    
    total_reviews = len(data)
    avg_score = round(sum(doc['Reviewer_Score'] for doc in data) / total_reviews, 2)
    
    # Most common nationality
    nationalities = [doc['Reviewer_Nationality'] for doc in data if doc.get('Reviewer_Nationality')]
    most_common_nationality = Counter(nationalities).most_common(1)[0][0] if nationalities else 'N/A'
    
    # Most reviewed hotel
    hotels = [doc['Hotel_Name'] for doc in data if doc.get('Hotel_Name')]
    most_reviewed_hotel = Counter(hotels).most_common(1)[0][0] if hotels else 'N/A'
    
    return {
        'total_reviews': total_reviews,
        'avg_score': avg_score,
        'most_common_nationality': most_common_nationality,
        'most_reviewed_hotel': most_reviewed_hotel
    }

@callback(
    [Output('metrics-cards', 'children'),
     Output('score-distribution-chart', 'figure'),
     Output('top-hotels-chart', 'figure'),
     Output('top-nationalities-chart', 'figure'),
     Output('data-table', 'children')],
    [Input('hotel-name-input', 'value'),
     Input('nationality-input', 'value'),
     Input('score-range-slider', 'value')]
)
def update_dashboard(hotel_name, nationality, score_range):
    # Query MongoDB
    data = query_mongodb(
        hotel_name=hotel_name,
        nationality=nationality,
        min_score=score_range[0],
        max_score=score_range[1]
    )
    
    # Calculate metrics
    metrics = calculate_metrics(data)
    
    # Create metrics cards
    metrics_cards = html.Div([
        html.Div([
            html.H3(f"{metrics['total_reviews']:,}", style={'margin': 0, 'color': '#3498db', 'fontSize': '2rem'}),
            html.P("Total Reviews", style={'margin': 0, 'color': '#7f8c8d'})
        ], style={
            'textAlign': 'center', 'padding': '20px', 'backgroundColor': 'white',
            'borderRadius': '8px', 'boxShadow': '0 2px 4px rgba(0,0,0,0.1)',
            'width': '23%', 'display': 'inline-block', 'margin': '1%'
        }),
        
        html.Div([
            html.H3(str(metrics['avg_score']), style={'margin': 0, 'color': '#e74c3c', 'fontSize': '2rem'}),
            html.P("Average Score", style={'margin': 0, 'color': '#7f8c8d'})
        ], style={
            'textAlign': 'center', 'padding': '20px', 'backgroundColor': 'white',
            'borderRadius': '8px', 'boxShadow': '0 2px 4px rgba(0,0,0,0.1)',
            'width': '23%', 'display': 'inline-block', 'margin': '1%'
        }),
        
        html.Div([
            html.H3(metrics['most_common_nationality'][:20], style={'margin': 0, 'color': '#9b59b6', 'fontSize': '1.2rem'}),
            html.P("Top Nationality", style={'margin': 0, 'color': '#7f8c8d'})
        ], style={
            'textAlign': 'center', 'padding': '20px', 'backgroundColor': 'white',
            'borderRadius': '8px', 'boxShadow': '0 2px 4px rgba(0,0,0,0.1)',
            'width': '23%', 'display': 'inline-block', 'margin': '1%'
        }),
        
        html.Div([
            html.H3(metrics['most_reviewed_hotel'][:20], style={'margin': 0, 'color': '#f39c12', 'fontSize': '1.2rem'}),
            html.P("Top Hotel", style={'margin': 0, 'color': '#7f8c8d'})
        ], style={
            'textAlign': 'center', 'padding': '20px', 'backgroundColor': 'white',
            'borderRadius': '8px', 'boxShadow': '0 2px 4px rgba(0,0,0,0.1)',
            'width': '23%', 'display': 'inline-block', 'margin': '1%'
        })
    ])
    
    # Score Distribution Chart
    if data:
        scores = [doc['Reviewer_Score'] for doc in data]
        score_counts = Counter(scores)
        score_dist_fig = px.bar(
            x=list(score_counts.keys()),
            y=list(score_counts.values()),
            title="Score Distribution",
            labels={'x': 'Score', 'y': 'Count'},
            color_discrete_sequence=['#3498db']
        )
        score_dist_fig.update_layout(
            plot_bgcolor='white',
            paper_bgcolor='white',
            font={'size': 12},
            title={'x': 0.5},
            showlegend=False
        )
    else:
        score_dist_fig = px.bar(title="Score Distribution - No Data")
        score_dist_fig.update_layout(plot_bgcolor='white', paper_bgcolor='white')
    
    # Top Hotels Chart
    if data:
        hotels = [doc['Hotel_Name'] for doc in data if doc.get('Hotel_Name')]
        hotel_counts = Counter(hotels).most_common(10)
        top_hotels_fig = px.bar(
            x=[count for _, count in hotel_counts],
            y=[hotel[:25] + '...' if len(hotel) > 25 else hotel for hotel, _ in hotel_counts],
            orientation='h',
            title="Top Hotels",
            labels={'x': 'Reviews', 'y': 'Hotel'},
            color_discrete_sequence=['#e74c3c']
        )
        top_hotels_fig.update_layout(
            plot_bgcolor='white',
            paper_bgcolor='white',
            font={'size': 12},
            title={'x': 0.5},
            yaxis={'categoryorder': 'total ascending'},
            showlegend=False
        )
    else:
        top_hotels_fig = px.bar(title="Top Hotels - No Data")
        top_hotels_fig.update_layout(plot_bgcolor='white', paper_bgcolor='white')
    
    # Top Nationalities Chart
    if data:
        nationalities = [doc['Reviewer_Nationality'] for doc in data if doc.get('Reviewer_Nationality')]
        nationality_counts = Counter(nationalities).most_common(10)
        top_nationalities_fig = px.bar(
            x=[nationality for nationality, _ in nationality_counts],
            y=[count for _, count in nationality_counts],
            title="Top Nationalities",
            labels={'x': 'Nationality', 'y': 'Count'},
            color_discrete_sequence=['#9b59b6']
        )
        top_nationalities_fig.update_layout(
            plot_bgcolor='white',
            paper_bgcolor='white',
            font={'size': 12},
            title={'x': 0.5},
            showlegend=False
        )
    else:
        top_nationalities_fig = px.bar(title="Top Nationalities - No Data")
        top_nationalities_fig.update_layout(plot_bgcolor='white', paper_bgcolor='white')
    
    # Simple Data Table
    if data:
        table_data = data[:20]  # Show 20 records
        
        if table_data:
            table_rows = []
            headers = ['Hotel Name', 'Nationality', 'Score', 'Review']
            
            # Table header
            header_style = {'backgroundColor': '#f8f9fa', 'fontWeight': 'bold', 'padding': '12px', 'border': '1px solid #dee2e6'}
            table_header = html.Tr([html.Th(header, style=header_style) for header in headers])
            
            # Table rows
            for doc in table_data:
                cell_style = {'padding': '12px', 'border': '1px solid #dee2e6', 'fontSize': '14px'}
                row = html.Tr([
                    html.Td(doc.get('Hotel_Name', 'N/A')[:30], style=cell_style),
                    html.Td(doc.get('Reviewer_Nationality', 'N/A'), style=cell_style),
                    html.Td(doc.get('Reviewer_Score', 'N/A'), style=cell_style),
                    html.Td((doc.get('Positive_Review', 'N/A')[:60] + '...' 
                            if doc.get('Positive_Review') and len(doc.get('Positive_Review', '')) > 60 
                            else doc.get('Positive_Review', 'N/A')), style=cell_style)
                ])
                table_rows.append(row)
            
            data_table = html.Div([
                html.H3("Sample Reviews", style={'marginBottom': 15, 'color': '#2c3e50'}),
                html.Table([
                    html.Thead(table_header),
                    html.Tbody(table_rows)
                ], style={'width': '100%', 'borderCollapse': 'collapse', 'backgroundColor': 'white', 'borderRadius': '8px'})
            ])
        else:
            data_table = html.Div("No data available")
    else:
        data_table = html.Div("No data available")
    
    return metrics_cards, score_dist_fig, top_hotels_fig, top_nationalities_fig, data_table

if __name__ == '__main__':
    app.run(debug=True)