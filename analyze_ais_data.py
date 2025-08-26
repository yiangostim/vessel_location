#!/usr/bin/env python3
"""
AIS Dry Bulk Vessel Data Analyzer
Analyzes the collected AIS data and generates comprehensive insights

Usage: python analyze_ais_data.py [--csv-path path] [--days N] [--export-html]
"""

import pandas as pd
import numpy as np
import json
import os
import argparse
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, List, Tuple
import warnings
warnings.filterwarnings('ignore')

class AISDataAnalyzer:
    def __init__(self, csv_path: str = "ais_data/dry_bulk_vessels.csv", 
                 vessel_db_path: str = "ais_data/vessel_database.json"):
        self.csv_path = csv_path
        self.vessel_db_path = vessel_db_path
        self.df = None
        self.vessel_db = {}
        
        # Set up plotting style
        plt.style.use('default')
        sns.set_palette("husl")
        
    def load_data(self):
        """Load AIS data and vessel database"""
        print(f"Loading data from {self.csv_path}...")
        
        if not os.path.exists(self.csv_path):
            raise FileNotFoundError(f"CSV file not found: {self.csv_path}")
        
        # Load CSV data
        self.df = pd.read_csv(self.csv_path)
        self.df['timestamp'] = pd.to_datetime(self.df['timestamp'])
        
        # Load vessel database if available
        if os.path.exists(self.vessel_db_path):
            with open(self.vessel_db_path, 'r') as f:
                vessel_list = json.load(f)
                self.vessel_db = {str(v['mmsi']): v for v in vessel_list}
        
        print(f"Loaded {len(self.df)} position records")
        print(f"Loaded {len(self.vessel_db)} vessels in database")
        print(f"Data time range: {self.df['timestamp'].min()} to {self.df['timestamp'].max()}")
        
    def filter_by_days(self, days: int):
        """Filter data to last N days"""
        cutoff_date = datetime.now() - timedelta(days=days)
        initial_count = len(self.df)
        self.df = self.df[self.df['timestamp'] >= cutoff_date]
        print(f"Filtered to last {days} days: {len(self.df)} records (was {initial_count})")
        
    def basic_statistics(self):
        """Generate basic statistics"""
        print("\n" + "="*60)
        print("BASIC STATISTICS")
        print("="*60)
        
        unique_vessels = self.df['mmsi'].nunique()
        total_records = len(self.df)
        
        print(f"Total Position Records: {total_records:,}")
        print(f"Unique Vessels: {unique_vessels:,}")
        print(f"Average Records per Vessel: {total_records/unique_vessels:.1f}")
        
        # Time analysis
        time_span = (self.df['timestamp'].max() - self.df['timestamp'].min()).total_seconds() / 3600
        print(f"Data Time Span: {time_span:.1f} hours")
        print(f"Records per Hour: {total_records/time_span:.1f}")
        
        # Geographic coverage
        lat_range = self.df['latitude'].max() - self.df['latitude'].min()
        lon_range = self.df['longitude'].max() - self.df['longitude'].min()
        print(f"Geographic Coverage: {lat_range:.1f}° latitude × {lon_range:.1f}° longitude")
        
    def vessel_size_analysis(self):
        """Analyze vessel sizes and DWT distribution"""
        print("\n" + "="*60)
        print("VESSEL SIZE ANALYSIS")
        print("="*60)
        
        # Get vessels with DWT data
        dwt_data = self.df.dropna(subset=['estimated_dwt'])
        
        if len(dwt_data) == 0:
            print("No DWT data available in the dataset")
            return
            
        print(f"Vessels with DWT data: {dwt_data['mmsi'].nunique():,}")
        print(f"DWT Range: {dwt_data['estimated_dwt'].min():,.0f} - {dwt_data['estimated_dwt'].max():,.0f} tonnes")
        print(f"Average DWT: {dwt_data['estimated_dwt'].mean():,.0f} tonnes")
        print(f"Median DWT: {dwt_data['estimated_dwt'].median():,.0f} tonnes")
        
        # DWT distribution by bins
        bins = [(40000, 50000), (50000, 60000), (60000, 70000), 
                (70000, 80000), (80000, 90000), (90000, 100000)]
        
        print("\nDWT Distribution:")
        for min_dwt, max_dwt in bins:
            count = len(dwt_data[(dwt_data['estimated_dwt'] >= min_dwt) & 
                                (dwt_data['estimated_dwt'] < max_dwt)])
            pct = (count / len(dwt_data)) * 100
            bar = "█" * int(pct / 2)  # Scale for display
            print(f"{min_dwt:,}-{max_dwt:,}t: {count:3d} ({pct:4.1f}%) {bar}")
            
    def activity_analysis(self):
        """Analyze vessel activity patterns"""
        print("\n" + "="*60)
        print("ACTIVITY ANALYSIS")
        print("="*60)
        
        # Speed analysis
        speed_data = self.df['speed_knots'].dropna()
        print(f"Speed Statistics (knots):")
        print(f"  Average: {speed_data.mean():.1f}")
        print(f"  Median: {speed_data.median():.1f}")
        print(f"  Max: {speed_data.max():.1f}")
        
        # Activity classification
        stationary = (speed_data < 1).sum()
        slow = ((speed_data >= 1) & (speed_data < 5)).sum()
        cruising = ((speed_data >= 5) & (speed_data < 12)).sum()
        fast = (speed_data >= 12).sum()
        
        print(f"\nActivity Classification:")
        print(f"  Stationary (<1 kt): {stationary:,} ({stationary/len(speed_data)*100:.1f}%)")
        print(f"  Slow (1-5 kt): {slow:,} ({slow/len(speed_data)*100:.1f}%)")
        print(f"  Cruising (5-12 kt): {cruising:,} ({cruising/len(speed_data)*100:.1f}%)")
        print(f"  Fast (>12 kt): {fast:,} ({fast/len(speed_data)*100:.1f}%)")
        
        # Time-based analysis
        self.df['hour'] = self.df['timestamp'].dt.hour
        hourly_activity = self.df.groupby('hour').size()
        peak_hour = hourly_activity.idxmax()
        print(f"\nPeak Activity Hour: {peak_hour:02d}:00 UTC ({hourly_activity[peak_hour]} records)")
        
    def geographic_analysis(self):
        """Analyze geographic distribution"""
        print("\n" + "="*60)
        print("GEOGRAPHIC ANALYSIS")
        print("="*60)
        
        # Regional classification (simplified)
        regions = {
            'North Atlantic': (self.df['latitude'] > 40) & (self.df['longitude'] < -20),
            'Mediterranean': (self.df['latitude'].between(30, 46)) & (self.df['longitude'].between(-6, 36)),
            'North Sea/Baltic': (self.df['latitude'] > 50) & (self.df['longitude'].between(-5, 30)),
            'Asia-Pacific': (self.df['longitude'] > 100),
            'Americas': (self.df['longitude'] < -30),
            'Other': True  # Default for remaining
        }
        
        print("Regional Distribution:")
        total_records = len(self.df)
        for region, condition in regions.items():
            if region == 'Other':
                # Calculate 'Other' as remainder
                other_count = total_records - sum([len(self.df[cond]) for cond in list(regions.values())[:-1]])
                count = other_count
            else:
                count = len(self.df[condition])
            pct = (count / total_records) * 100
            print(f"  {region}: {count:,} ({pct:.1f}%)")
            
        # Most active areas (by density)
        print(f"\nMost Northern Point: {self.df['latitude'].max():.2f}°N")
        print(f"Most Southern Point: {self.df['latitude'].min():.2f}°N")
        print(f"Most Eastern Point: {self.df['longitude'].max():.2f}°E")
        print(f"Most Western Point: {self.df['longitude'].min():.2f}°W")
        
    def vessel_insights(self):
        """Generate insights about individual vessels"""
        print("\n" + "="*60)
        print("VESSEL INSIGHTS")
        print("="*60)
        
        # Most tracked vessels
        vessel_counts = self.df['mmsi'].value_counts().head(10)
        print("Most Frequently Tracked Vessels:")
        for mmsi, count in vessel_counts.items():
            vessel_info = self.vessel_db.get(str(mmsi), {})
            name = vessel_info.get('name', self.df[self.df['mmsi']==mmsi]['vessel_name'].iloc[0])
            dwt = vessel_info.get('estimated_dwt')
            dwt_str = f", {dwt:,}t DWT" if dwt else ""
            print(f"  {name} ({mmsi}): {count} positions{dwt_str}")
            
        # Speed champions
        print(f"\nFastest Recorded Speeds:")
        fastest = self.df.nlargest(5, 'speed_knots')[['vessel_name', 'mmsi', 'speed_knots', 'timestamp']]
        for _, row in fastest.iterrows():
            print(f"  {row['vessel_name']} ({row['mmsi']}): {row['speed_knots']:.1f} knots")
            
        # Journey analysis
        vessel_journeys = self.df.groupby('mmsi').agg({
            'latitude': ['min', 'max'],
            'longitude': ['min', 'max'],
            'timestamp': ['min', 'max'],
            'speed_knots': 'mean'
        }).round(2)
        
        # Calculate distances traveled (very rough approximation)
        print(f"\nLongest Tracking Periods:")
        journey_durations = []
        for mmsi in self.df['mmsi'].unique()[:10]:  # Top 10 most tracked
            vessel_data = self.df[self.df['mmsi'] == mmsi].sort_values('timestamp')
            if len(vessel_data) > 1:
                duration = (vessel_data['timestamp'].iloc[-1] - vessel_data['timestamp'].iloc[0]).total_seconds() / 3600
                journey_durations.append((mmsi, duration, len(vessel_data)))
        
        journey_durations.sort(key=lambda x: x[1], reverse=True)
        for mmsi, hours, count in journey_durations[:5]:
            vessel_name = self.df[self.df['mmsi']==mmsi]['vessel_name'].iloc[0]
            print(f"  {vessel_name} ({mmsi}): {hours:.1f} hours ({count} positions)")
            
    def destination_analysis(self):
        """Analyze vessel destinations"""
        print("\n" + "="*60)
        print("DESTINATION ANALYSIS")
        print("="*60)
        
        # Clean destination data
        destinations = self.df[self.df['destination'].notna() & 
                             (self.df['destination'] != 'Unknown') & 
                             (self.df['destination'] != '')]['destination']
        
        if len(destinations) == 0:
            print("No destination data available")
            return
            
        dest_counts = destinations.value_counts().head(10)
        print("Top Destinations:")
        for dest, count in dest_counts.items():
            vessels = self.df[self.df['destination'] == dest]['mmsi'].nunique()
            print(f"  {dest}: {vessels} vessels, {count} position reports")
            
    def temporal_analysis(self):
        """Analyze temporal patterns"""
        print("\n" + "="*60)
        print("TEMPORAL PATTERNS")
        print("="*60)
        
        # Daily patterns
        self.df['day_of_week'] = self.df['timestamp'].dt.day_name()
        daily_activity = self.df.groupby('day_of_week').size()
        print("Activity by Day of Week:")
        for day in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']:
            if day in daily_activity:
                count = daily_activity[day]
                bar = "█" * int(count / daily_activity.max() * 20)
                print(f"  {day[:3]}: {count:4d} {bar}")
                
        # Monthly trends (if data spans multiple months)
        self.df['month'] = self.df['timestamp'].dt.strftime('%Y-%m')
        monthly_activity = self.df.groupby('month').size()
        if len(monthly_activity) > 1:
            print(f"\nMonthly Activity Trend:")
            for month, count in monthly_activity.items():
                vessels = self.df[self.df['month'] == month]['mmsi'].nunique()
                print(f"  {month}: {count:,} positions from {vessels} vessels")
                
    def generate_summary_report(self, output_file: str = None):
        """Generate complete analysis report"""
        if output_file:
            import sys
            original_stdout = sys.stdout
            sys.stdout = open(output_file, 'w')
            
        try:
            print("AIS DRY BULK VESSEL DATA ANALYSIS REPORT")
            print("=" * 80)
            print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"Data source: {self.csv_path}")
            print()
            
            self.basic_statistics()
            self.vessel_size_analysis()
            self.activity_analysis()
            self.geographic_analysis()
            self.vessel_insights()
            self.destination_analysis()
            self.temporal_analysis()
            
            print("\n" + "="*80)
            print("END OF REPORT")
            
        finally:
            if output_file:
                sys.stdout.close()
                sys.stdout = original_stdout
                print(f"Report saved to {output_file}")
                
    def create_visualizations(self, output_dir: str = "plots"):
        """Create data visualizations"""
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        # 1. Vessel positions map
        plt.figure(figsize=(15, 10))
        plt.scatter(self.df['longitude'], self.df['latitude'], 
                   c=self.df['speed_knots'], cmap='viridis', alpha=0.6, s=1)
        plt.colorbar(label='Speed (knots)')
        plt.xlabel('Longitude')
        plt.ylabel('Latitude')
        plt.title('Dry Bulk Vessel Positions Colored by Speed')
        plt.grid(True, alpha=0.3)
        plt.savefig(f'{output_dir}/vessel_positions_map.png', dpi=300, bbox_inches='tight')
        plt.close()
        
        # 2. Speed distribution
        plt.figure(figsize=(10, 6))
        self.df['speed_knots'].hist(bins=50, alpha=0.7, edgecolor='black')
        plt.xlabel('Speed (knots)')
        plt.ylabel('Frequency')
        plt.title('Vessel Speed Distribution')
        plt.grid(True, alpha=0.3)
        plt.savefig(f'{output_dir}/speed_distribution.png', dpi=300, bbox_inches='tight')
        plt.close()
        
        # 3. DWT distribution (if available)
        dwt_data = self.df.dropna(subset=['estimated_dwt'])
        if len(dwt_data) > 0:
            plt.figure(figsize=(10, 6))
            dwt_data['estimated_dwt'].hist(bins=30, alpha=0.7, edgecolor='black')
            plt.xlabel('Estimated DWT (tonnes)')
            plt.ylabel('Frequency')
            plt.title('Vessel DWT Distribution')
            plt.grid(True, alpha=0.3)
            plt.savefig(f'{output_dir}/dwt_distribution.png', dpi=300, bbox_inches='tight')
            plt.close()
        
        # 4. Activity timeline
        daily_activity = self.df.groupby(self.df['timestamp'].dt.date).size()
        plt.figure(figsize=(12, 6))
        daily_activity.plot(kind='line', marker='o')
        plt.xlabel('Date')
        plt.ylabel('Position Reports')
        plt.title('Daily AIS Activity Timeline')
        plt.grid(True, alpha=0.3)
        plt.xticks(rotation=45)
        plt.savefig(f'{output_dir}/activity_timeline.png', dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"Visualizations saved to {output_dir}/ directory")

def main():
    parser = argparse.ArgumentParser(description='Analyze AIS dry bulk vessel data')
    parser.add_argument('--csv-path', default='ais_data/dry_bulk_vessels.csv',
                       help='Path to CSV data file')
    parser.add_argument('--days', type=int, help='Filter to last N days')
    parser.add_argument('--export-report', help='Export report to text file')
    parser.add_argument('--create-plots', action='store_true',
                       help='Create visualization plots')
    parser.add_argument('--plots-dir', default='plots',
                       help='Directory for plot outputs')
    
    args = parser.parse_args()
    
    try:
        analyzer = AISDataAnalyzer(args.csv_path)
        analyzer.load_data()
        
        if args.days:
            analyzer.filter_by_days(args.days)
            
        if args.export_report:
            analyzer.generate_summary_report(args.export_report)
        else:
            analyzer.generate_summary_report()
            
        if args.create_plots:
            analyzer.create_visualizations(args.plots_dir)
            
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Make sure the CSV file exists and the path is correct.")
        return 1
    except Exception as e:
        print(f"Error analyzing data: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
