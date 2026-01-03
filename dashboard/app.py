"""
Sentinet Dashboard
Professional-grade dark mode Streamlit dashboard for market surveillance.
"""

import asyncio
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List
import yaml
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import networkx as nx
from dotenv import load_dotenv

from lib.db_client import DBClient

load_dotenv()

# Page configuration
st.set_page_config(
    page_title="Sentinet - Market Surveillance",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Dark mode CSS
st.markdown("""
<style>
    .main {
        background-color: #0e1117;
    }
    .stApp {
        background-color: #0e1117;
    }
    h1, h2, h3 {
        color: #00ff88;
        font-family: 'Courier New', monospace;
    }
    .metric-card {
        background: linear-gradient(135deg, #1a1d29 0%, #2d3142 100%);
        padding: 20px;
        border-radius: 10px;
        border: 1px solid #00ff88;
        box-shadow: 0 4px 6px rgba(0, 255, 136, 0.2);
    }
    .alert-critical {
        background-color: #ff0055;
        color: white;
        padding: 10px;
        border-radius: 5px;
        margin: 5px 0;
        font-weight: bold;
    }
    .alert-warning {
        background-color: #ffaa00;
        color: black;
        padding: 10px;
        border-radius: 5px;
        margin: 5px 0;
        font-weight: bold;
    }
    .alert-info {
        background-color: #0088ff;
        color: white;
        padding: 10px;
        border-radius: 5px;
        margin: 5px 0;
    }
    .throughput-gauge {
        font-size: 48px;
        font-weight: bold;
        color: #00ff88;
        text-align: center;
    }
</style>
""", unsafe_allow_html=True)


class SentinetDashboard:
    """Main dashboard class."""
    
    def __init__(self):
        # Load configuration
        with open('config/config.yaml', 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Database client
        self.db = DBClient(
            host=os.getenv('DB_HOST', 'localhost'),
            port=int(os.getenv('DB_PORT', 5432)),
            database=os.getenv('DB_NAME', 'sentinet'),
            user=os.getenv('DB_USER', 'sentinet_user'),
            password=os.getenv('DB_PASSWORD', 'sentinet_pass')
        )
        
        self.refresh_interval = self.config['dashboard']['refresh_interval_seconds']
        self.max_chart_points = self.config['dashboard']['max_chart_points']
        self.min_correlation = self.config['dashboard']['network_graph_min_correlation']
    
    async def connect(self):
        """Connect to database."""
        await self.db.connect()
    
    async def get_recent_ticks(self, symbol: str, minutes: int = 5) -> pd.DataFrame:
        """Get recent ticks for a symbol."""
        ticks = await self.db.query_recent_ticks(symbol, minutes)
        
        if not ticks:
            return pd.DataFrame()
        
        df = pd.DataFrame(ticks)
        df['time'] = pd.to_datetime(df['time'])
        return df.tail(self.max_chart_points)
    
    async def get_recent_alerts(self, minutes: int = 10) -> List[Dict]:
        """Get recent alerts."""
        return await self.db.query_recent_alerts(minutes)
    
    async def get_correlation_matrix(self, minutes: int = 5) -> pd.DataFrame:
        """Get correlation matrix."""
        correlations = await self.db.query_correlation_matrix(minutes)
        
        if not correlations:
            return pd.DataFrame()
        
        # Convert to matrix format
        symbols = set()
        for corr in correlations:
            symbols.add(corr['symbol_a'])
            symbols.add(corr['symbol_b'])
        
        symbols = sorted(list(symbols))
        n = len(symbols)
        
        # Initialize matrix with 1.0 on diagonal
        matrix = np.eye(n)
        
        # Fill matrix
        symbol_to_idx = {s: i for i, s in enumerate(symbols)}
        
        for corr in correlations:
            i = symbol_to_idx[corr['symbol_a']]
            j = symbol_to_idx[corr['symbol_b']]
            val = corr['correlation']
            
            matrix[i, j] = val
            matrix[j, i] = val  # Symmetric
        
        return pd.DataFrame(matrix, index=symbols, columns=symbols)
    
    def create_price_chart(self, symbol: str, df: pd.DataFrame) -> go.Figure:
        """Create price chart with Bollinger Bands."""
        if df.empty:
            fig = go.Figure()
            fig.update_layout(
                title=f"{symbol} - No Data",
                template="plotly_dark",
                paper_bgcolor='#0e1117',
                plot_bgcolor='#1a1d29'
            )
            return fig
        
        # Calculate Bollinger Bands
        df['sma'] = df['price'].rolling(window=20, min_periods=1).mean()
        df['std'] = df['price'].rolling(window=20, min_periods=1).std()
        df['upper_band'] = df['sma'] + (2 * df['std'])
        df['lower_band'] = df['sma'] - (2 * df['std'])
        
        fig = go.Figure()
        
        # Upper band
        fig.add_trace(go.Scatter(
            x=df['time'],
            y=df['upper_band'],
            name='Upper Band',
            line=dict(color='rgba(255, 0, 85, 0.3)', width=1),
            fill=None
        ))
        
        # Lower band
        fig.add_trace(go.Scatter(
            x=df['time'],
            y=df['lower_band'],
            name='Lower Band',
            line=dict(color='rgba(255, 0, 85, 0.3)', width=1),
            fill='tonexty',
            fillcolor='rgba(255, 0, 85, 0.1)'
        ))
        
        # SMA
        fig.add_trace(go.Scatter(
            x=df['time'],
            y=df['sma'],
            name='SMA',
            line=dict(color='#ffaa00', width=2)
        ))
        
        # Price
        fig.add_trace(go.Scatter(
            x=df['time'],
            y=df['price'],
            name='Price',
            line=dict(color='#00ff88', width=3)
        ))
        
        fig.update_layout(
            title=f"{symbol} - Price with Bollinger Bands",
            xaxis_title="Time",
            yaxis_title="Price ($)",
            template="plotly_dark",
            paper_bgcolor='#0e1117',
            plot_bgcolor='#1a1d29',
            height=400,
            hovermode='x unified',
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )
        
        return fig
    
    def create_correlation_heatmap(self, corr_matrix: pd.DataFrame) -> go.Figure:
        """Create correlation heatmap."""
        if corr_matrix.empty:
            fig = go.Figure()
            fig.update_layout(
                title="Correlation Matrix - No Data",
                template="plotly_dark",
                paper_bgcolor='#0e1117',
                plot_bgcolor='#1a1d29'
            )
            return fig
        
        fig = go.Figure(data=go.Heatmap(
            z=corr_matrix.values,
            x=corr_matrix.columns,
            y=corr_matrix.index,
            colorscale=[
                [0, '#ff0055'],      # Red for negative correlation
                [0.5, '#1a1d29'],    # Dark for zero
                [1, '#00ff88']       # Green for positive correlation
            ],
            zmid=0,
            text=corr_matrix.values,
            texttemplate='%{text:.2f}',
            textfont={"size": 10},
            colorbar=dict(title="Correlation")
        ))
        
        fig.update_layout(
            title="Asset Correlation Matrix",
            template="plotly_dark",
            paper_bgcolor='#0e1117',
            plot_bgcolor='#1a1d29',
            height=500
        )
        
        return fig
    
    def create_network_graph(self, corr_matrix: pd.DataFrame) -> go.Figure:
        """Create NetworkX graph visualization of asset relationships."""
        if corr_matrix.empty:
            fig = go.Figure()
            fig.update_layout(
                title="Asset Network - No Data",
                template="plotly_dark",
                paper_bgcolor='#0e1117',
                plot_bgcolor='#1a1d29'
            )
            return fig
        
        # Create graph
        G = nx.Graph()
        
        # Add nodes
        symbols = list(corr_matrix.columns)
        G.add_nodes_from(symbols)
        
        # Add edges where correlation > threshold
        for i, symbol_a in enumerate(symbols):
            for j, symbol_b in enumerate(symbols):
                if i < j:  # Avoid duplicates
                    correlation = corr_matrix.iloc[i, j]
                    if abs(correlation) > self.min_correlation:
                        G.add_edge(symbol_a, symbol_b, weight=abs(correlation))
        
        # Layout
        pos = nx.spring_layout(G, k=2, iterations=50)
        
        # Create edge traces
        edge_traces = []
        for edge in G.edges(data=True):
            x0, y0 = pos[edge[0]]
            x1, y1 = pos[edge[1]]
            weight = edge[2]['weight']
            
            edge_trace = go.Scatter(
                x=[x0, x1, None],
                y=[y0, y1, None],
                mode='lines',
                line=dict(
                    width=weight * 5,
                    color=f'rgba(0, 255, 136, {weight})'
                ),
                hoverinfo='none',
                showlegend=False
            )
            edge_traces.append(edge_trace)
        
        # Create node trace
        node_x = []
        node_y = []
        node_text = []
        
        for node in G.nodes():
            x, y = pos[node]
            node_x.append(x)
            node_y.append(y)
            node_text.append(node)
        
        node_trace = go.Scatter(
            x=node_x,
            y=node_y,
            mode='markers+text',
            text=node_text,
            textposition="top center",
            textfont=dict(size=14, color='#00ff88'),
            marker=dict(
                size=30,
                color='#00ff88',
                line=dict(width=2, color='#ffffff')
            ),
            hoverinfo='text',
            showlegend=False
        )
        
        # Create figure
        fig = go.Figure(data=edge_traces + [node_trace])
        
        fig.update_layout(
            title="Asset Correlation Network (Edge Width = Correlation Strength)",
            template="plotly_dark",
            paper_bgcolor='#0e1117',
            plot_bgcolor='#1a1d29',
            height=600,
            showlegend=False,
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)
        )
        
        return fig
    
    def render_alerts(self, alerts: List[Dict]):
        """Render alert feed."""
        if not alerts:
            st.info("No recent alerts")
            return
        
        for alert in alerts[:20]:  # Show last 20
            severity = alert['severity']
            message = alert['message']
            timestamp = alert['time'].strftime('%H:%M:%S')
            
            if severity == 'CRITICAL':
                css_class = 'alert-critical'
                icon = '🚨'
            elif severity == 'WARNING':
                css_class = 'alert-warning'
                icon = '⚠️'
            else:
                css_class = 'alert-info'
                icon = 'ℹ️'
            
            st.markdown(
                f'<div class="{css_class}">{icon} [{timestamp}] {message}</div>',
                unsafe_allow_html=True
            )
    
    async def run(self):
        """Run the dashboard."""
        # Header
        st.title("🎯 SENTINET")
        st.markdown("### High-Frequency Market Microstructure Surveillance Engine")
        
        # Sidebar configuration
        st.sidebar.header("Configuration")
        watched_symbols = st.sidebar.multiselect(
            "Watched Symbols",
            options=self.config['symbols']['equities'] + self.config['symbols']['crypto'],
            default=['AAPL', 'BTC/USD']
        )
        
        lookback_minutes = st.sidebar.slider("Lookback Window (minutes)", 1, 30, 5)
        
        st.sidebar.markdown("---")
        st.sidebar.markdown("**System Status**")
        st.sidebar.success("🟢 Database Connected")
        st.sidebar.success("🟢 Redpanda Streaming")
        
        # Create placeholders for dynamic updates
        header_placeholder = st.empty()
        chart_placeholder = st.empty()
        corr_heatmap_placeholder = st.empty()
        network_graph_placeholder = st.empty()
        alerts_placeholder = st.empty()
        
        # Main update loop (using st.empty() as per user constraint)
        while True:
            try:
                # Update header with throughput
                with header_placeholder.container():
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        st.markdown('<div class="throughput-gauge">⚡</div>', unsafe_allow_html=True)
                        st.metric("System Status", "OPERATIONAL", delta="Real-time")
                    
                    with col2:
                        st.metric("Watched Symbols", len(watched_symbols))
                    
                    with col3:
                        current_time = datetime.now().strftime('%H:%M:%S')
                        st.metric("Last Update", current_time)
                
                # Price charts
                with chart_placeholder.container():
                    st.subheader("📊 Real-Time Price Analysis")
                    
                    if watched_symbols:
                        cols = st.columns(min(2, len(watched_symbols)))
                        
                        for idx, symbol in enumerate(watched_symbols[:4]):  # Max 4 charts
                            with cols[idx % 2]:
                                df = await self.get_recent_ticks(symbol, lookback_minutes)
                                fig = self.create_price_chart(symbol, df)
                                st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.warning("No symbols selected")
                
                # Correlation analysis
                corr_matrix = await self.get_correlation_matrix(lookback_minutes)
                
                with corr_heatmap_placeholder.container():
                    st.subheader("🔥 Correlation Heatmap")
                    fig = self.create_correlation_heatmap(corr_matrix)
                    st.plotly_chart(fig, use_container_width=True)
                
                with network_graph_placeholder.container():
                    st.subheader("🕸️ Asset Correlation Network")
                    fig = self.create_network_graph(corr_matrix)
                    st.plotly_chart(fig, use_container_width=True)
                
                # Alerts
                with alerts_placeholder.container():
                    st.subheader("🚨 Risk Feed")
                    alerts = await self.get_recent_alerts(10)
                    self.render_alerts(alerts)
                
                # Sleep before next update
                await asyncio.sleep(self.refresh_interval)
            
            except Exception as e:
                st.error(f"Error updating dashboard: {e}")
                await asyncio.sleep(5)


async def main():
    """Main entry point."""
    dashboard = SentinetDashboard()
    await dashboard.connect()
    await dashboard.run()


if __name__ == '__main__':
    asyncio.run(main())
