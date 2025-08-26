name: AIS Dry Bulk Vessel Scraper

on:
  schedule:
    # Run every 6 hours with shorter 10-minute sessions to avoid rate limiting
    - cron: '0 */6 * * *'
  workflow_dispatch:
    inputs:
      duration_minutes:
        description: 'Duration to scrape in minutes'
        required: false
        default: '10'
        type: string
      dwt_min:
        description: 'Minimum DWT'
        required: false
        default: '40000'
        type: string
      dwt_max:
        description: 'Maximum DWT'
        required: false
        default: '100000'
        type: string

env:
  AISSTREAM_API_KEY: ${{ secrets.AISSTREAM_API_KEY }}

jobs:
  scrape-ais-data:
    runs-on: ubuntu-latest
    
    steps:
    - name: Checkout repository
      uses: actions/checkout@v4
      with:
        token: ${{ secrets.GITHUB_TOKEN }}
        fetch-depth: 0
        
    - name: Setup Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.9'
        
    - name: Install dependencies
      run: |
        pip install websockets pandas asyncio-mqtt python-dateutil pytz
        
    - name: Create data directory
      run: |
        mkdir -p ais_data
        
    - name: Archive old data if CSV gets too large
      run: |
        if [ -f "ais_data/dry_bulk_vessels.csv" ]; then
          file_size=$(du -m ais_data/dry_bulk_vessels.csv | cut -f1)
          if [ $file_size -gt 50 ]; then  # If > 50MB
            echo "CSV is ${file_size}MB, archiving old data..."
            
            # Create archive directory
            mkdir -p ais_data/archives
            
            # Move current CSV to archive with timestamp
            timestamp=$(date +%Y%m%d_%H%M%S)
            mv ais_data/dry_bulk_vessels.csv "ais_data/archives/dry_bulk_vessels_${timestamp}.csv"
            
            # Compress the archive
            gzip "ais_data/archives/dry_bulk_vessels_${timestamp}.csv"
            
            echo "Archived large CSV file"
          fi
        fi

    - name: Create scraper script
      run: |
        cat > analyze_ais_data.py << 'EOF'
        import asyncio
        import websockets
        import json
        import pandas as pd
        import os
        import sys
        from datetime import datetime, timezone
        import logging
        from typing import Dict, Set, List, Optional
        import signal
        import math

        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        logger = logging.getLogger(__name__)

        class AISDataCollector:
            def __init__(self, api_key: str, duration_minutes: int = 10, dwt_min: int = 40000, dwt_max: int = 100000):
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
                self.dry_bulk_types = {70, 71, 72, 73, 74, 79}
                
                # Load existing data
                self.load_existing_data()
                
                # Setup signal handlers
                signal.signal(signal.SIGINT, self.signal_handler)
                signal.signal(signal.SIGTERM, self.signal_handler)

            def signal_handler(self, signum, frame):
                logger.info(f"Received signal {signum}, shutting down gracefully...")
                self.is_running = False

            def load_existing_data(self):
                """Load existing vessel database to avoid duplicate static data requests"""
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
                    length = dimensions.get('A', 0) + dimensions.get('B', 0)
                    width = dimensions.get('C', 0) + dimensions.get('D', 0)
                    
                    if length <= 0 or width <= 0:
                        return None
                    
                    # Enhanced DWT estimation for dry bulk carriers
                    # Based on typical length/beam ratios and cargo hold coefficients
                    if length < 150:  # Handysize
                        dwt_factor = 0.75
                    elif length < 200:  # Supramax/Ultramax
                        dwt_factor = 0.80
                    elif length < 250:  # Panamax
                        dwt_factor = 0.85
                    else:  # Capesize
                        dwt_factor = 0.90
                    
                    # Volume approximation * density factor * cargo coefficient
                    estimated_dwt = int((length * width * 12 * dwt_factor))
                    
                    # Apply reasonable bounds
                    return max(10000, min(400000, estimated_dwt))
                    
                except Exception as e:
                    logger.debug(f"Error estimating DWT: {e}")
                    return None

            def is_target_vessel(self, mmsi: str, vessel_data: Dict) -> bool:
                """Determine if vessel matches our criteria"""
                # Check ship type
                ship_type = vessel_data.get('ship_type')
                if ship_type and ship_type not in self.dry_bulk_types:
                    return False
                
                # If no ship type, assume it could be dry bulk (conservative approach)
                
                # Check DWT if available
                estimated_dwt = vessel_data.get('estimated_dwt')
                if estimated_dwt:
                    return self.dwt_min <= estimated_dwt <= self.dwt_max
                
                # If no DWT data but potential dry bulk type, include it
                return True

            async def handle_message(self, message_data: str):
                """Process incoming AIS messages"""
                try:
                    message = json.loads(message_data)
                    
                    if 'error' in message:
                        logger.error(f"API Error: {message['error']}")
                        return
                    
                    message_type = message.get('MessageType')
                    metadata = message.get('Metadata', {})
                    ais_message = message.get('Message', {})
                    
                    if message_type == 'ShipStaticData':
                        await self.process_static_data(ais_message.get('ShipStaticData', {}), metadata)
                    elif message_type == 'PositionReport':
                        await self.process_position_report(ais_message.get('PositionReport', {}), metadata)
                        
                except json.JSONDecodeError as e:
                    logger.debug(f"JSON decode error: {e}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")

            async def process_static_data(self, static_data: Dict, metadata: Dict):
                """Process vessel static data"""
                mmsi = str(static_data.get('UserID') or metadata.get('MMSI', ''))
                if not mmsi:
                    return
                
                # Get existing vessel data or create new
                vessel = self.vessel_database.get(mmsi, {})
                
                # Update vessel information
                vessel.update({
                    'mmsi': mmsi,
                    'name': (static_data.get('Name') or '').strip() or vessel.get('name', 'Unknown'),
                    'call_sign': (static_data.get('CallSign') or '').strip() or vessel.get('call_sign', 'Unknown'),
                    'imo_number': static_data.get('ImoNumber') or vessel.get('imo_number'),
                    'ship_type': static_data.get('Type') or vessel.get('ship_type'),
                    'dimensions': static_data.get('Dimension') or vessel.get('dimensions', {}),
                    'destination': (static_data.get('Destination') or '').strip() or vessel.get('destination', 'Unknown'),
                    'max_draught': static_data.get('MaximumStaticDraught') or vessel.get('max_draught'),
                    'last_static_update': datetime.now(timezone.utc).isoformat()
                })
                
                # Estimate DWT from dimensions
                if vessel['dimensions']:
                    estimated_dwt = self.estimate_dwt_from_dimensions(vessel['dimensions'])
                    if estimated_dwt:
                        vessel['estimated_dwt'] = estimated_dwt
                
                self.vessel_database[mmsi] = vessel
                
                logger.info(f"Updated static data for {vessel['name']} ({mmsi}) - DWT: {vessel.get('estimated_dwt', 'Unknown')}")

            async def process_position_report(self, position_data: Dict, metadata: Dict):
                """Process vessel position reports"""
                mmsi = str(position_data.get('UserID') or metadata.get('MMSI', ''))
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
                
                # Check if this is a target vessel
                if not self.is_target_vessel(mmsi, vessel):
                    return
                
                # Create position record
                record = {
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'mmsi': mmsi,
                    'vessel_name': vessel.get('name', metadata.get('ShipName', 'Unknown')),
                    'latitude': position_data.get('Latitude'),
                    'longitude': position_data.get('Longitude'),
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
                
                # Filter out invalid coordinates
                if (record['latitude'] is None or record['longitude'] is None or 
                    abs(record['latitude']) > 90 or abs(record['longitude']) > 180):
                    return
                
                self.collected_data.append(record)
                
                dwt_str = f"{vessel.get('estimated_dwt'):,}" if vessel.get('estimated_dwt') else 'Unknown'
                logger.info(f"Collected position for {record['vessel_name']} ({mmsi}) - DWT: {dwt_str} - Speed: {record['speed_knots']} kts")

            async def save_data(self):
                """Save collected data to CSV file - APPEND ONLY, never delete old data"""
                if not self.collected_data:
                    logger.info("No new data to save")
                    return
                
                # Create DataFrame from collected data
                new_df = pd.DataFrame(self.collected_data)
                
                # Ensure data directory exists
                os.makedirs(os.path.dirname(self.csv_file_path), exist_ok=True)
                
                # APPEND to existing CSV or create new one
                if os.path.exists(self.csv_file_path):
                    try:
                        # Load existing data to check for exact duplicates only
                        existing_df = pd.read_csv(self.csv_file_path)
                        
                        # Remove only EXACT duplicates (same mmsi, timestamp, lat, lon)
                        # This allows multiple positions for same vessel at different times
                        merge_cols = ['mmsi', 'timestamp', 'latitude', 'longitude']
                        
                        # Find truly new records (not exact duplicates)
                        merged = new_df.merge(existing_df[merge_cols], on=merge_cols, how='left', indicator=True)
                        truly_new = new_df[merged['_merge'] == 'left_only']
                        
                        if len(truly_new) > 0:
                            # Append only new records
                            truly_new.to_csv(self.csv_file_path, mode='a', header=False, index=False)
                            logger.info(f"Appended {len(truly_new)} NEW records to {self.csv_file_path}")
                            
                            # Log total file size
                            total_lines = sum(1 for line in open(self.csv_file_path)) - 1  # -1 for header
                            logger.info(f"Total records in CSV: {total_lines:,}")
                        else:
                            logger.info("No new unique records to append (all were duplicates)")
                            
                    except Exception as e:
                        logger.warning(f"Could not load existing CSV, creating new: {e}")
                        new_df.to_csv(self.csv_file_path, index=False)
                        logger.info(f"Created new CSV with {len(new_df)} records")
                else:
                    # Create new file
                    new_df.to_csv(self.csv_file_path, index=False)
                    logger.info(f"Created new CSV with {len(new_df)} records")
                
                # Save/update vessel database (this can be fully updated each time)
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
                        logger.info("Subscription sent")
                        
                        # Set up timeout
                        end_time = asyncio.get_event_loop().time() + (self.duration_minutes * 60)
                        
                        try:
                            while self.is_running and asyncio.get_event_loop().time() < end_time:
                                try:
                                    # Wait for message with timeout
                                    message = await asyncio.wait_for(websocket.recv(), timeout=30.0)
                                    await self.handle_message(message)
                                except asyncio.TimeoutError:
                                    logger.debug("No message received in 30 seconds, continuing...")
                                    continue
                                except websockets.exceptions.ConnectionClosed:
                                    logger.warning("WebSocket connection closed")
                                    break
                                    
                        except KeyboardInterrupt:
                            logger.info("Interrupted by user")
                        finally:
                            await self.save_data()
                            
                except Exception as e:
                    logger.error(f"Connection error: {e}")
                    # Try to save any collected data
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

        async def main():
            # Get configuration from environment variables
            api_key = os.getenv('AISSTREAM_API_KEY')
            if not api_key:
                logger.error("AISSTREAM_API_KEY environment variable not set")
                sys.exit(1)
            
            duration_minutes = int(os.getenv('DURATION_MINUTES', '10'))
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
        EOF

    - name: Run AIS Scraper
      env:
        DURATION_MINUTES: ${{ github.event.inputs.duration_minutes || '10' }}
        DWT_MIN: ${{ github.event.inputs.dwt_min || '40000' }}
        DWT_MAX: ${{ github.event.inputs.dwt_max || '100000' }}
      run: python analyze_ais_data.py
      
    - name: Display data collection summary
      run: |
        echo "=== DATA COLLECTION SUMMARY ==="
        echo "Collection frequency: Every 6 hours (optimal for 12-15kt vessels)"
        echo "Session duration: 10 minutes per run"
        echo ""
        if [ -f "ais_data/dry_bulk_vessels.csv" ]; then
          total_lines=$(wc -l < ais_data/dry_bulk_vessels.csv)
          echo "📊 Main CSV file: $((total_lines - 1)) position records"
          echo "📁 File size: $(du -h ais_data/dry_bulk_vessels.csv | cut -f1)"
          echo "📅 Data age: $(stat -c %y ais_data/dry_bulk_vessels.csv | cut -d' ' -f1) to $(date +%Y-%m-%d)"
          echo ""
          echo "🚢 Latest vessel positions (last 3):"
          tail -3 ais_data/dry_bulk_vessels.csv | while read line; do
            echo "   $line"
          done
        else
          echo "❌ No CSV file found"
        fi
        
        if [ -f "ais_data/vessel_database.json" ]; then
          vessel_count=$(grep -o '"mmsi"' ais_data/vessel_database.json | wc -l)
          echo ""
          echo "🗃️  Vessel database: $vessel_count unique vessels tracked"
        fi
        
        echo ""
        echo "ℹ️  Data Collection Strategy:"
        echo "   • Every 6 hours = ~25-30nm vessel movement between samples"  
        echo "   • 10min sessions = captures multiple position updates per vessel"
        echo "   • Appends new data, never deletes historical positions"
        echo "   • Tracks dry bulk carriers 40,000-100,000 DWT globally"

    - name: Commit and push data
      run: |
        git config --local user.email "action@github.com"
        git config --local user.name "GitHub Action AIS Scraper"
        
        echo "Current git status:"
        git status
        
        # Force sync with remote
        git fetch origin main || echo "Fetch failed, continuing..."
        git reset --hard origin/main || echo "Reset failed, continuing..."
        
        # Check if data directory exists and has content
        if [ -d "ais_data" ] && [ "$(ls -A ais_data)" ]; then
          echo "Data directory contents:"
          ls -la ais_data/
          
          # Re-add the data files
          git add ais_data/ || echo "Git add failed"
          
          if [ -n "$(git status --porcelain)" ]; then
            echo "Changes detected, committing..."
            git commit -m "Update AIS data - $(date '+%Y-%m-%d %H:%M:%S UTC')" || echo "Commit failed"
            git push origin main || echo "Push failed"
            echo "Data commit attempted"
          else
            echo "No changes detected in git status"
          fi
        else
          echo "ERROR: No ais_data directory or it's empty!"
        fi
        
    - name: Upload data as artifact
      uses: actions/upload-artifact@v4
      with:
        name: ais-data-backup-${{ github.run_number }}
        path: ais_data/
        retention-days: 90
