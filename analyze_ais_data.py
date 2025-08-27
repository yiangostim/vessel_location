#!/usr/bin/env python3
"""
AIS Dry Bulk Vessel Data Collector
Connects to AISStream.io and collects position data for dry bulk carriers
"""

import asyncio
import websockets
import json
import pandas as pd
import os
import sys
from datetime import datetime, timezone
import logging
import signal
from typing import Dict, List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AISDataCollector:
    def __init__(self, api_key: str, duration_minutes: int = 120, dwt_min: int = 40000, dwt_max: int = 100000):
        self.api_key = api_key
        self.duration_minutes = duration_minutes
        self.dwt_min = dwt_min
        self.dwt_max = dwt_max
        self.vessel_database: Dict = {}
        self.collected_data: List[Dict] = []
        self.is_running = True
        self.csv_file_path = "ais_data/dry_bulk_vessels.csv"
        self.vessel_db_path = "ais_data/vessel_database.json"
        
        # Dry bulk vessel types (AIS ship type codes)
        # 70-79 are cargo ship types, which include bulk carriers
        self.dry_bulk_types = {70, 71, 72, 73, 74, 79}
        
        # Load existing data
        self.load_existing_data()
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.is_running = False

    def load_existing_data(self):
        """Load existing vessel database to avoid duplicate requests"""
        if os.path.exists(self.vessel_db_path):
            try:
                with open(self.vessel_db_path, 'r') as f:
                    data = json.load(f)
                    self.vessel_database = {str(v['mmsi']): v for v in data}
                logger.info(f"Loaded {len(self.vessel_database)} vessels from existing database")
            except Exception as e:
                logger.warning(f"Could not load existing vessel database: {e}")

    def estimate_dwt_from_dimensions(self, dimensions: Dict) -> Optional[int]:
        """Estimate DWT from vessel dimensions using naval architecture principles"""
        try:
            if not dimensions:
                return None
                
            # AIS dimension format: A+B = length, C+D = width
            length = dimensions.get('A', 0) + dimensions.get('B', 0)
            width = dimensions.get('C', 0) + dimensions.get('D', 0)
            
            if length <= 0 or width <= 0:
                return None
            
            # DWT estimation factors based on vessel size categories
            if length < 150:  # Handysize
                dwt_factor = 0.75
            elif length < 200:  # Supramax/Ultramax
                dwt_factor = 0.80
            elif length < 250:  # Panamax
                dwt_factor = 0.85
            else:  # Capesize
                dwt_factor = 0.90
            
            # Estimate DWT: Volume approximation * density factor * cargo coefficient
            estimated_dwt = int(length * width * 12 * dwt_factor)
            
            # Apply reasonable bounds
            return max(10000, min(400000, estimated_dwt))
            
        except Exception as e:
            logger.debug(f"Error estimating DWT: {e}")
            return None

    def is_target_vessel(self, mmsi: str, vessel_data: Dict) -> bool:
        """Determine if vessel matches our target criteria"""
        # Check ship type first
        ship_type = vessel_data.get('ship_type')
        if ship_type and ship_type not in self.dry_bulk_types:
            return False
        
        # Check DWT range if available
        estimated_dwt = vessel_data.get('estimated_dwt')
        if estimated_dwt:
            return self.dwt_min <= estimated_dwt <= self.dwt_max
        
        # If no clear type but dimensions suggest bulk carrier, include it
        dimensions = vessel_data.get('dimensions')
        if dimensions:
            estimated_dwt = self.estimate_dwt_from_dimensions(dimensions)
            if estimated_dwt:
                return self.dwt_min <= estimated_dwt <= self.dwt_max
        
        # Conservative approach: include vessels without clear data
        return True

    async def handle_message(self, message_data: str):
        """Process incoming AIS messages"""
        try:
            message = json.loads(message_data)
            
            if 'error' in message:
                logger.error(f"API Error: {message['error']}")
                return
            
            message_type = message.get('MessageType')
            
            if message_type == 'ShipStaticData':
                await self.process_static_data(message)
            elif message_type == 'PositionReport':
                await self.process_position_report(message)
                
        except json.JSONDecodeError as e:
            logger.debug(f"JSON decode error: {e}")
        except Exception as e:
            logger.error(f"Error processing message: {e}")

    async def process_static_data(self, message: Dict):
        """Process vessel static data messages"""
        try:
            metadata = message.get('Metadata', {})
            static_data = message.get('Message', {}).get('ShipStaticData', {})
            
            mmsi = str(metadata.get('MMSI') or static_data.get('UserID', ''))
            if not mmsi:
                return
            
            # Get existing vessel data or create new entry
            vessel = self.vessel_database.get(mmsi, {})
            
            # Update vessel information
            vessel.update({
                'mmsi': mmsi,
                'name': (static_data.get('Name') or metadata.get('ShipName', '')).strip() or vessel.get('name', 'Unknown'),
                'call_sign': (static_data.get('CallSign') or '').strip() or vessel.get('call_sign', 'Unknown'),
                'imo_number': static_data.get('ImoNumber') or vessel.get('imo_number'),
                'ship_type': static_data.get('Type') or vessel.get('ship_type'),
                'dimensions': static_data.get('Dimension') or vessel.get('dimensions', {}),
                'destination': (static_data.get('Destination') or '').strip() or vessel.get('destination', 'Unknown'),
                'max_draught': static_data.get('MaximumStaticDraught') or vessel.get('max_draught'),
                'last_static_update': datetime.now(timezone.utc).isoformat()
            })
            
            # Estimate DWT from dimensions if available
            if vessel['dimensions']:
                estimated_dwt = self.estimate_dwt_from_dimensions(vessel['dimensions'])
                if estimated_dwt:
                    vessel['estimated_dwt'] = estimated_dwt
            
            self.vessel_database[mmsi] = vessel
            
            dwt_info = f"{vessel.get('estimated_dwt', 'Unknown')}"
            if vessel.get('estimated_dwt'):
                dwt_info = f"{vessel['estimated_dwt']:,}"
            
            logger.info(f"Updated static data: {vessel['name']} ({mmsi}) - DWT: {dwt_info}")
            
        except Exception as e:
            logger.debug(f"Error processing static data: {e}")

    async def process_position_report(self, message: Dict):
        """Process vessel position reports"""
        try:
            metadata = message.get('Metadata', {})
            position_data = message.get('Message', {}).get('PositionReport', {})
            
            mmsi = str(metadata.get('MMSI') or position_data.get('UserID', ''))
            if not mmsi:
                return
            
            # Ensure vessel exists in database
            if mmsi not in self.vessel_database:
                self.vessel_database[mmsi] = {
                    'mmsi': mmsi,
                    'name': metadata.get('ShipName', 'Unknown'),
                    'ship_type': None,
                    'estimated_dwt': None
                }
            
            vessel = self.vessel_database[mmsi]
            
            # Check if this vessel matches our target criteria
            if not self.is_target_vessel(mmsi, vessel):
                return
            
            # Validate coordinates
            lat = position_data.get('Latitude')
            lon = position_data.get('Longitude')
            
            if lat is None or lon is None or abs(lat) > 90 or abs(lon) > 180:
                return
            
            # Create position record
            record = {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'mmsi': mmsi,
                'vessel_name': vessel.get('name', metadata.get('ShipName', 'Unknown')),
                'latitude': lat,
                'longitude': lon,
                'speed_knots': position_data.get('Sog', 0),
                'course_degrees': position_data.get('Cog', 0),
                'heading_degrees': position_data.get('TrueHeading'),
                'navigation_status': position_data.get('NavigationalStatus'),
                'ship_type': vessel.get('ship_type'),
                'estimated_dwt': vessel.get('estimated_dwt'),
                'call_sign': vessel.get('call_sign', 'Unknown'),
                'destination': vessel.get('destination', 'Unknown'),
                'rate_of_turn': position_data.get('RateOfTurn'),
                'position_accuracy': position_data.get('PositionAccuracy', False),
                'imo_number': vessel.get('imo_number'),
                'max_draught': vessel.get('max_draught')
            }
            
            self.collected_data.append(record)
            
            dwt_str = f"{vessel.get('estimated_dwt'):,}" if vessel.get('estimated_dwt') else 'Unknown'
            logger.info(f"Position collected: {record['vessel_name']} ({mmsi}) - DWT: {dwt_str} - Speed: {record['speed_knots']} kts")
            
        except Exception as e:
            logger.debug(f"Error processing position report: {e}")

    async def save_data(self):
        """Save collected data to CSV file (append mode to preserve historical data)"""
        if not self.collected_data:
            logger.info("No new data to save")
            return
        
        # Create DataFrame from collected data
        new_df = pd.DataFrame(self.collected_data)
        
        # Ensure data directory exists
        os.makedirs(os.path.dirname(self.csv_file_path), exist_ok=True)
        
        # Handle CSV file - append new data
        if os.path.exists(self.csv_file_path):
            try:
                existing_df = pd.read_csv(self.csv_file_path)
                
                # Remove exact duplicates only (same mmsi, timestamp, lat, lon)
                merge_cols = ['mmsi', 'timestamp', 'latitude', 'longitude']
                merged = new_df.merge(existing_df[merge_cols], on=merge_cols, how='left', indicator=True)
                truly_new = new_df[merged['_merge'] == 'left_only']
                
                if len(truly_new) > 0:
                    truly_new.to_csv(self.csv_file_path, mode='a', header=False, index=False)
                    logger.info(f"Appended {len(truly_new)} new records to CSV")
                    
                    # Log total file size
                    total_lines = sum(1 for line in open(self.csv_file_path)) - 1
                    logger.info(f"Total records in CSV: {total_lines:,}")
                else:
                    logger.info("No new unique records to append")
                    
            except Exception as e:
                logger.warning(f"Could not load existing CSV: {e}")
                new_df.to_csv(self.csv_file_path, index=False)
                logger.info(f"Created new CSV with {len(new_df)} records")
        else:
            new_df.to_csv(self.csv_file_path, index=False)
            logger.info(f"Created new CSV with {len(new_df)} records")
        
        # Save vessel database
        vessel_list = list(self.vessel_database.values())
        with open(self.vessel_db_path, 'w') as f:
            json.dump(vessel_list, f, indent=2, default=str)
        logger.info(f"Updated vessel database with {len(vessel_list)} vessels")

    async def run(self):
        """Main execution loop"""
        logger.info(f"Starting AIS data collection for {self.duration_minutes} minutes")
        logger.info(f"Target DWT range: {self.dwt_min:,} - {self.dwt_max:,}")
        
        uri = "wss://stream.aisstream.io/v0/stream"
        
        try:
            async with websockets.connect(uri) as websocket:
                logger.info("Connected to AISStream")
                
                # Subscribe to global AIS data
                subscription = {
                    "APIKey": self.api_key,
                    "BoundingBoxes": [[[-90, -180], [90, 180]]],  # Global coverage
                    "FilterMessageTypes": ["PositionReport", "ShipStaticData"]
                }
                
                await websocket.send(json.dumps(subscription))
                logger.info("Subscription sent - listening for messages...")
                
                # Set up collection timeout
                end_time = asyncio.get_event_loop().time() + (self.duration_minutes * 60)
                
                try:
                    while self.is_running and asyncio.get_event_loop().time() < end_time:
                        try:
                            message = await asyncio.wait_for(websocket.recv(), timeout=30.0)
                            await self.handle_message(message)
                        except asyncio.TimeoutError:
                            logger.debug("No message in 30 seconds, continuing...")
                            continue
                        except websockets.exceptions.ConnectionClosed:
                            logger.warning("WebSocket connection closed")
                            break
                            
                except KeyboardInterrupt:
                    logger.info("Collection interrupted by user")
                finally:
                    await self.save_data()
                    
        except Exception as e:
            logger.error(f"Connection error: {e}")
            await self.save_data()
            raise

    def print_summary(self):
        """Print collection summary"""
        target_vessels = [v for v in self.vessel_database.values() 
                         if self.is_target_vessel(v['mmsi'], v)]
        
        print(f"\n{'='*60}")
        print("AIS DATA COLLECTION SUMMARY")
        print(f"{'='*60}")
        print(f"Collection Duration: {self.duration_minutes} minutes")
        print(f"New Position Records: {len(self.collected_data):,}")
        print(f"Total Vessels in Database: {len(self.vessel_database):,}")
        print(f"Target Vessels (Dry Bulk {self.dwt_min:,}-{self.dwt_max:,} DWT): {len(target_vessels):,}")
        
        if target_vessels:
            dwt_values = [v['estimated_dwt'] for v in target_vessels if v.get('estimated_dwt')]
            if dwt_values:
                print(f"Average DWT of targets: {sum(dwt_values)/len(dwt_values):,.0f}")
                print(f"DWT Range of targets: {min(dwt_values):,} - {max(dwt_values):,}")
        print(f"{'='*60}")

async def main():
    """Main entry point"""
    # Get configuration from environment variables or defaults
    api_key = os.getenv('AISSTREAM_API_KEY')
    if not api_key:
        logger.error("AISSTREAM_API_KEY environment variable not set")
        print("Please set your AISStream API key as an environment variable:")
        print("export AISSTREAM_API_KEY='your_api_key_here'")
        sys.exit(1)
    
    duration_minutes = int(os.getenv('DURATION_MINUTES', '120'))
    dwt_min = int(os.getenv('DWT_MIN', '40000'))
    dwt_max = int(os.getenv('DWT_MAX', '100000'))
    
    collector = AISDataCollector(api_key, duration_minutes, dwt_min, dwt_max)
    
    try:
        await collector.run()
        collector.print_summary()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
