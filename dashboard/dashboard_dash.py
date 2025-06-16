import dash
from dash import dcc, html, Input, Output, callback
import plotly.express as px
import plotly.graph_objects as go
from pymongo import MongoClient
from collections import Counter
import pandas as pd

from database.mongo_storage import MongoDatabaseClient

client = MongoDatabaseClient
collection = client.init_mongodb_connection()

# Initialize Dash app
app = dash.Dash(__name__)
app.title = "Hotel Reviews Dashboard"

# Define the layout
app.layout = html.Div([
    html.Div([
        html.H1("Hotel Reviews Dashboard", 
                style={'textAlign': 'center', 'marginBottom': 30, 'color': '#2c3e50'}),
        html.P("This dashboard allows you to filter and explore hotel reviews stored in MongoDB. "
               "Use the filters below to narrow down your search by hotel name, reviewer nationality, and review score.",
               style={'textAlign': 'center', 'marginBottom': 30, 'color': '#7f8c8d'})
    ]),
    
    # Filters Section
    html.Div([
        html.H3("Filters & Controls", style={'marginBottom': 20, 'color': '#34495e'}),
        
        html.Div([
            html.Label("Hotel Name:", style={'fontWeight': 'bold', 'marginBottom': 5}),
            dcc.Input(
                id='hotel-name-input',
                type='text',
                placeholder='Enter hotel name...',
                style={'width': '100%', 'padding': '8px', 'marginBottom': 15}
            )
        ]),
        
        html.Div([
            html.Label("Nationality:", style={'fontWeight': 'bold', 'marginBottom': 5}),
            dcc.Input(
                id='nationality-input',
                type='text',
                placeholder='Enter nationality...',
                style={'width': '100%', 'padding': '8px', 'marginBottom': 15}
            )
        ]),
        
        html.Div([
            html.Label("Score Range:", style={'fontWeight': 'bold', 'marginBottom': 5}),
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
        'width': '25%', 
        'display': 'inline-block', 
        'verticalAlign': 'top',
        'padding': '20px',
        'backgroundColor': '#f8f9fa',
        'borderRadius': '10px',
        'margin': '10px'
    }),
    
    # Main Content Section
    html.Div([
        # Metrics Cards
        html.Div(id='metrics-cards', style={'marginBottom': 30}),
        
        # Charts Section
        html.Div([
            html.Div([
                dcc.Graph(id='score-distribution-chart')
            ], style={'width': '50%', 'display': 'inline-block'}),
            
            html.Div([
                dcc.Graph(id='top-hotels-chart')
            ], style={'width': '50%', 'display': 'inline-block'})
        ]),
        
        html.Div([
            dcc.Graph(id='top-nationalities-chart')
        ], style={'width': '100%', 'marginTop': 20}),
        
        # Data Table
        html.Div(id='data-table', style={'marginTop': 30})
        
    ], style={
        'width': '70%', 
        'display': 'inline-block', 
        'verticalAlign': 'top',
        'padding': '20px'
    })
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
            html.H3(str(metrics['total_reviews']), style={'margin': 0, 'color': '#29b5e8'}),
            html.P("Total Reviews", style={'margin': 0, 'color': '#7f8c8d'})
        ], style={
            'textAlign': 'center', 'padding': '20px', 'backgroundColor': 'white',
            'borderRadius': '10px', 'boxShadow': '0 2px 4px rgba(0,0,0,0.1)',
            'width': '22%', 'display': 'inline-block', 'margin': '1%'
        }),
        
        html.Div([
            html.H3(str(metrics['avg_score']), style={'margin': 0, 'color': '#FF9F36'}),
            html.P("Average Score", style={'margin': 0, 'color': '#7f8c8d'})
        ], style={
            'textAlign': 'center', 'padding': '20px', 'backgroundColor': 'white',
            'borderRadius': '10px', 'boxShadow': '0 2px 4px rgba(0,0,0,0.1)',
            'width': '22%', 'display': 'inline-block', 'margin': '1%'
        }),
        
        html.Div([
            html.H3(metrics['most_common_nationality'], style={'margin': 0, 'color': '#D45B90', 'fontSize': '16px'}),
            html.P("Most Common Nationality", style={'margin': 0, 'color': '#7f8c8d'})
        ], style={
            'textAlign': 'center', 'padding': '20px', 'backgroundColor': 'white',
            'borderRadius': '10px', 'boxShadow': '0 2px 4px rgba(0,0,0,0.1)',
            'width': '22%', 'display': 'inline-block', 'margin': '1%'
        }),
        
        html.Div([
            html.H3(metrics['most_reviewed_hotel'], style={'margin': 0, 'color': '#7D44CF', 'fontSize': '16px'}),
            html.P("Most Reviewed Hotel", style={'margin': 0, 'color': '#7f8c8d'})
        ], style={
            'textAlign': 'center', 'padding': '20px', 'backgroundColor': 'white',
            'borderRadius': '10px', 'boxShadow': '0 2px 4px rgba(0,0,0,0.1)',
            'width': '22%', 'display': 'inline-block', 'margin': '1%'
        })
    ])
    
    # Score Distribution Chart
    if data:
        scores = [doc['Reviewer_Score'] for doc in data]
        score_counts = Counter(scores)
        score_dist_fig = px.bar(
            x=list(score_counts.keys()),
            y=list(score_counts.values()),
            title="Reviewer Score Distribution",
            labels={'x': 'Score', 'y': 'Count'}
        )
        score_dist_fig.update_layout(showlegend=False)
    else:
        score_dist_fig = px.bar(title="Reviewer Score Distribution - No Data")
    
    # Top Hotels Chart
    if data:
        hotels = [doc['Hotel_Name'] for doc in data if doc.get('Hotel_Name')]
        hotel_counts = Counter(hotels).most_common(10)
        top_hotels_fig = px.bar(
            x=[count for _, count in hotel_counts],
            y=[hotel for hotel, _ in hotel_counts],
            orientation='h',
            title="Top 10 Hotels by Number of Reviews",
            labels={'x': 'Number of Reviews', 'y': 'Hotel'}
        )
        top_hotels_fig.update_layout(showlegend=False, yaxis={'categoryorder': 'total ascending'})
    else:
        top_hotels_fig = px.bar(title="Top 10 Hotels - No Data")
    
    # Top Nationalities Chart
    if data:
        nationalities = [doc['Reviewer_Nationality'] for doc in data if doc.get('Reviewer_Nationality')]
        nationality_counts = Counter(nationalities).most_common(10)
        top_nationalities_fig = px.bar(
            x=[nationality for nationality, _ in nationality_counts],
            y=[count for _, count in nationality_counts],
            title="Top 10 Reviewer Nationalities",
            labels={'x': 'Nationality', 'y': 'Count'}
        )
        top_nationalities_fig.update_layout(showlegend=False)
    else:
        top_nationalities_fig = px.bar(title="Top 10 Nationalities - No Data")
    
    # Data Table (show first 100 records)
    if data:
        table_data = data[:100]  # Limit to first 100 records for performance
        table_rows = []
        
        # Table header
        if table_data:
            headers = ['Hotel Name', 'Reviewer Nationality', 'Reviewer Score', 'Review']
            table_header = html.Tr([html.Th(header) for header in headers])
            
            # Table rows
            for doc in table_data:
                row = html.Tr([
                    html.Td(doc.get('Hotel_Name', 'N/A')),
                    html.Td(doc.get('Reviewer_Nationality', 'N/A')),
                    html.Td(doc.get('Reviewer_Score', 'N/A')),
                    html.Td(doc.get('Positive_Review', 'N/A')[:100] + '...' if doc.get('Positive_Review') and len(doc.get('Positive_Review', '')) > 100 else doc.get('Positive_Review', 'N/A'))
                ])
                table_rows.append(row)
            
            data_table = html.Div([
                html.H3("Review Data (First 100 records)", style={'marginBottom': 20}),
                html.Table([
                    html.Thead(table_header),
                    html.Tbody(table_rows)
                ], style={
                    'width': '100%',
                    'borderCollapse': 'collapse',
                    'border': '1px solid #ddd'
                })
            ])
        else:
            data_table = html.Div("No data available")
    else:
        data_table = html.Div("No data available")
    
    return metrics_cards, score_dist_fig, top_hotels_fig, top_nationalities_fig, data_table

if __name__ == '__main__':
    app.run(debug=True)