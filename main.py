"""
File: mass_migration_50k.py
SISTEM LENGKAP untuk migrasi 50K member AMAN & TERKENDALI
"""
import asyncio
import time
import random
import sqlite3
import json
import hashlib
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import logging
from dataclasses import dataclass, asdict
import pandas as pd
from telethon import TelegramClient, errors
from telethon.tl.functions.channels import (
    GetParticipantsRequest, 
    InviteToChannelRequest,
    GetFullChannelRequest
)
from telethon.tl.types import (
    ChannelParticipantsSearch,
    InputPeerChannel,
    InputPeerUser,
    UserStatusRecently,
    UserStatusLastWeek,
    UserStatusLastMonth
)
from telethon.tl.functions.users import GetFullUserRequest

# ===== KONFIGURASI LOGGING =====
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'migration_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ===== DATA CLASSES =====
@dataclass
class MigrationConfig:
    """Konfigurasi migrasi"""
    source_group: str
    target_group: str
    total_members: int = 50000
    days_to_complete: int = 7
    max_daily_invites: int = 400
    max_hourly_invites: int = 80
    delay_between_invites: Tuple[float, float] = (3.0, 8.0)
    break_after_batch: int = 50
    break_duration: Tuple[int, int] = (30, 60)

@dataclass
class MigrationStats:
    """Statistik migrasi"""
    total_members: int = 0
    extracted_members: int = 0
    invited_today: int = 0
    invited_total: int = 0
    failed_today: int = 0
    failed_total: int = 0
    remaining_members: int = 0
    estimated_completion: Optional[datetime] = None
    success_rate: float = 0.0

# ===== DATABASE MANAGER =====
class MigrationDatabase:
    """Manager database SQLite untuk migrasi"""
    
    def __init__(self, db_path: str = 'migration_50k.db'):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Initialize database schema"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Table untuk members
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS members (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER UNIQUE,
                    username TEXT,
                    phone TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    is_bot INTEGER DEFAULT 0,
                    status TEXT, -- recently, last_week, last_month, long_time_ago
                    last_seen TIMESTAMP,
                    extracted_at TIMESTAMP,
                    priority INTEGER DEFAULT 5, -- 1=highest, 10=lowest
                    invited INTEGER DEFAULT 0,
                    invited_at TIMESTAMP NULL,
                    attempts INTEGER DEFAULT 0,
                    error_message TEXT NULL,
                    INDEX idx_priority (priority),
                    INDEX idx_invited (invited),
                    INDEX idx_status (status)
                )
            ''')
            
            # Table untuk daily progress
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS daily_progress (
                    date DATE PRIMARY KEY,
                    invited_count INTEGER DEFAULT 0,
                    failed_count INTEGER DEFAULT 0,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP NULL
                )
            ''')
            
            # Table untuk error logs
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS error_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    error_type TEXT,
                    error_message TEXT,
                    occurred_at TIMESTAMP,
                    resolved INTEGER DEFAULT 0,
                    FOREIGN KEY (user_id) REFERENCES members (user_id)
                )
            ''')
            
            # Table untuk system config
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS system_config (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    updated_at TIMESTAMP
                )
            ''')
            
            conn.commit()
    
    def save_member(self, member_data: Dict) -> bool:
        """Save member ke database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    INSERT OR REPLACE INTO members 
                    (user_id, username, phone, first_name, last_name, is_bot, 
                     status, last_seen, extracted_at, priority)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    member_data['user_id'],
                    member_data.get('username'),
                    member_data.get('phone'),
                    member_data.get('first_name', 'Unknown'),
                    member_data.get('last_name', ''),
                    member_data.get('is_bot', 0),
                    member_data.get('status', 'unknown'),
                    member_data.get('last_seen'),
                    datetime.now(),
                    member_data.get('priority', 5)
                ))
                
                conn.commit()
                return True
                
        except Exception as e:
            logger.error(f"Error saving member: {e}")
            return False
    
    def get_members_batch(self, limit: int = 100, priority: bool = True) -> List[Dict]:
        """Ambil batch members untuk diinvite"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            if priority:
                # Prioritaskan active users dulu
                cursor.execute('''
                    SELECT * FROM members 
                    WHERE invited = 0 AND attempts < 3
                    ORDER BY 
                        CASE status 
                            WHEN 'recently' THEN 1
                            WHEN 'last_week' THEN 2
                            WHEN 'last_month' THEN 3
                            ELSE 4
                        END,
                        priority ASC
                    LIMIT ?
                ''', (limit,))
            else:
                cursor.execute('''
                    SELECT * FROM members 
                    WHERE invited = 0 AND attempts < 3
                    LIMIT ?
                ''', (limit,))
            
            return [dict(row) for row in cursor.fetchall()]
    
    def update_member_status(self, user_id: int, success: bool, error_msg: str = None):
        """Update status member setelah invite attempt"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            if success:
                cursor.execute('''
                    UPDATE members 
                    SET invited = 1,
                        invited_at = ?,
                        attempts = attempts + 1
                    WHERE user_id = ?
                ''', (datetime.now(), user_id))
            else:
                cursor.execute('''
                    UPDATE members 
                    SET attempts = attempts + 1,
                        error_message = ?
                    WHERE user_id = ?
                ''', (error_msg, user_id))
            
            # Log error jika ada
            if error_msg and not success:
                cursor.execute('''
                    INSERT INTO error_logs 
                    (user_id, error_type, error_message, occurred_at)
                    VALUES (?, ?, ?, ?)
                ''', (user_id, 'invite_failed', error_msg, datetime.now()))
            
            conn.commit()
    
    def get_stats(self) -> MigrationStats:
        """Dapatkan statistik migrasi"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Hitung total
            cursor.execute('SELECT COUNT(*) FROM members')
            total_members = cursor.fetchone()[0]
            
            # Hitung sudah diinvite
            cursor.execute('SELECT COUNT(*) FROM members WHERE invited = 1')
            invited_total = cursor.fetchone()[0]
            
            # Hitung hari ini
            today = datetime.now().strftime('%Y-%m-%d')
            cursor.execute('''
                SELECT invited_count, failed_count 
                FROM daily_progress 
                WHERE date = ?
            ''', (today,))
            result = cursor.fetchone()
            invited_today = result[0] if result else 0
            failed_today = result[1] if result else 0
            
            # Hitung gagal total
            cursor.execute('SELECT COUNT(*) FROM members WHERE attempts >= 3 AND invited = 0')
            failed_total = cursor.fetchone()[0]
            
            # Estimasi completion
            remaining = total_members - invited_total
            if invited_today > 0:
                days_remaining = remaining / (invited_today or 1)
                estimated = datetime.now() + timedelta(days=days_remaining)
            else:
                estimated = None
            
            return MigrationStats(
                total_members=total_members,
                extracted_members=total_members,
                invited_today=invited_today,
                invited_total=invited_total,
                failed_today=failed_today,
                failed_total=failed_total,
                remaining_members=remaining,
                estimated_completion=estimated,
                success_rate=(invited_total / total_members * 100) if total_members > 0 else 0
            )
    
    def start_daily_session(self):
        """Start session harian"""
        today = datetime.now().strftime('%Y-%m-%d')
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR IGNORE INTO daily_progress (date, start_time)
                VALUES (?, ?)
            ''', (today, datetime.now()))
            conn.commit()
    
    def update_daily_progress(self, invited: int = 0, failed: int = 0):
        """Update progress harian"""
        today = datetime.now().strftime('%Y-%m-%d')
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE daily_progress 
                SET invited_count = invited_count + ?,
                    failed_count = failed_count + ?,
                    end_time = ?
                WHERE date = ?
            ''', (invited, failed, datetime.now(), today))
            conn.commit()

# ===== MIGRATION ENGINE =====
class MassMigrationEngine:
    """Engine utama untuk migrasi massal"""
    
    def __init__(self, api_id: int, api_hash: str, config: MigrationConfig):
        self.api_id = api_id
        self.api_hash = api_hash
        self.config = config
        self.client = None
        self.db = MigrationDatabase()
        
        # State management
        self.is_running = False
        self.current_day = 1
        self.invites_sent_today = 0
        self.last_reset_time = datetime.now()
        
        # Performance tracking
        self.start_time = None
        self.total_invites_sent = 0
        
        logger.info("MassMigrationEngine initialized")
    
    async def connect(self):
        """Connect ke Telegram"""
        try:
            self.client = TelegramClient(
                f'migration_{int(time.time())}',
                self.api_id,
                self.api_hash
            )
            
            await self.client.start()
            me = await self.client.get_me()
            logger.info(f"✅ Connected as: {me.first_name} (@{me.username})")
            logger.info(f"📱 Phone: {me.phone}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Connection failed: {e}")
            return False
    
    async def extract_all_members(self):
        """
        Extract SEMUA member dari grup sumber (50K)
        Dengan prioritas berdasarkan aktifitas
        """
        logger.info(f"🚀 Starting extraction from: {self.config.source_group}")
        
        try:
            # Dapatkan entity grup sumber
            source_entity = await self.client.get_entity(self.config.source_group)
            
            # Dapatkan info lengkap grup
            full_channel = await self.client(GetFullChannelRequest(source_entity))
            total_participants = getattr(full_channel.full_chat, 'participants_count', 0)
            logger.info(f"📊 Total participants: {total_participants}")
            
            # Setup progress tracking
            extracted_count = 0
            offset = 0
            limit = 200
            batch_number = 1
            
            # Start extraction loop
            while True:
                try:
                    logger.info(f"📦 Processing batch {batch_number}...")
                    
                    # Dapatkan participants
                    participants = await self.client(GetParticipantsRequest(
                        channel=source_entity,
                        filter=ChannelParticipantsSearch(''),
                        offset=offset,
                        limit=limit,
                        hash=0
                    ))
                    
                    if not participants or not participants.users:
                        logger.info("No more participants found")
                        break
                    
                    # Process setiap user
                    for user in participants.users:
                        # Tentukan status aktifitas
                        status = self._determine_user_status(user)
                        priority = self._calculate_priority(status)
                        
                        # Simpan ke database
                        member_data = {
                            'user_id': user.id,
                            'username': user.username,
                            'phone': user.phone,
                            'first_name': user.first_name,
                            'last_name': user.last_name or '',
                            'is_bot': 1 if user.bot else 0,
                            'status': status,
                            'last_seen': self._get_last_seen_date(user),
                            'priority': priority
                        }
                        
                        self.db.save_member(member_data)
                        extracted_count += 1
                        
                        # Progress update setiap 1000 member
                        if extracted_count % 1000 == 0:
                            logger.info(f"✅ Extracted: {extracted_count}/{total_participants}")
                    
                    offset += len(participants.users)
                    batch_number += 1
                    
                    # Delay untuk hindari flood
                    if len(participants.users) >= limit:
                        await asyncio.sleep(random.uniform(2, 5))
                    else:
                        break
                        
                except errors.FloodWaitError as e:
                    logger.warning(f"⚠️ Flood wait: {e.seconds} seconds")
                    await asyncio.sleep(e.seconds + 5)
                    
                except Exception as e:
                    logger.error(f"❌ Extraction error: {e}")
                    await asyncio.sleep(10)
            
            logger.info(f"🎉 Extraction completed! Total: {extracted_count} members")
            
            # Generate extraction report
            await self._generate_extraction_report()
            
            return extracted_count
            
        except Exception as e:
            logger.error(f"❌ Failed to extract members: {e}")
            return 0
    
    def _determine_user_status(self, user) -> str:
        """Tentukan status user berdasarkan last seen"""
        if not hasattr(user, 'status'):
            return 'long_time_ago'
        
        status = user.status
        if isinstance(status, UserStatusRecently):
            return 'recently'
        elif isinstance(status, UserStatusLastWeek):
            return 'last_week'
        elif isinstance(status, UserStatusLastMonth):
            return 'last_month'
        else:
            return 'long_time_ago'
    
    def _calculate_priority(self, status: str) -> int:
        """Hitung priority score"""
        priority_map = {
            'recently': 1,
            'last_week': 2,
            'last_month': 3,
            'long_time_ago': 5,
            'unknown': 5
        }
        return priority_map.get(status, 5)
    
    def _get_last_seen_date(self, user):
        """Dapatkan last seen date dari user"""
        if hasattr(user, 'status'):
            if hasattr(user.status, 'was_online'):
                return user.status.was_online
        return None
    
    async def start_migration(self):
        """Start proses migrasi utama"""
        if not self.client:
            await self.connect()
        
        logger.info("🚀 STARTING MASS MIGRATION (50K MEMBERS)")
        logger.info(f"📅 Estimated duration: {self.config.days_to_complete} days")
        logger.info(f"📊 Target per day: {self.config.max_daily_invites}")
        
        self.is_running = True
        self.start_time = datetime.now()
        
        # Start daily session
        self.db.start_daily_session()
        
        # Main migration loop
        day = 1
        while day <= self.config.days_to_complete and self.is_running:
            logger.info(f"\n{'='*60}")
            logger.info(f"📅 DAY {day}/{self.config.days_to_complete}")
            logger.info(f"{'='*60}")
            
            # Reset daily counter
            self.invites_sent_today = 0
            self.last_reset_time = datetime.now()
            
            # Jalankan migrasi untuk hari ini
            await self._migrate_day(day)
            
            # Update stats
            stats = self.db.get_stats()
            logger.info(f"📊 Day {day} Summary:")
            logger.info(f"  ✅ Invited today: {stats.invited_today}")
            logger.info(f"  ❌ Failed today: {stats.failed_today}")
            logger.info(f"  📈 Total invited: {stats.invited_total}/{stats.total_members}")
            logger.info(f"  ⏳ Remaining: {stats.remaining_members}")
            
            # Jika sudah selesai, break
            if stats.remaining_members <= 0:
                logger.info("🎉 All members have been invited!")
                break
            
            # Jika bukan hari terakhir, tunggu sampai besok
            if day < self.config.days_to_complete:
                logger.info("⏸️ Pausing until tomorrow...")
                await self._wait_until_tomorrow()
            
            day += 1
        
        # Migration completed
        await self._complete_migration()
    
    async def _migrate_day(self, day: int):
        """Eksekusi migrasi untuk satu hari"""
        # Dapatkan entity grup target
        target_entity = await self.client.get_entity(self.config.target_group)
        
        # Hitung target untuk hari ini
        stats = self.db.get_stats()
        daily_target = min(
            self.config.max_daily_invites,
            stats.remaining_members
        )
        
        logger.info(f"🎯 Daily target: {daily_target} invites")
        
        # Process dalam batch
        batch_size = 50
        batch_number = 1
        
        while self.invites_sent_today < daily_target and self.is_running:
            # Dapatkan batch members
            members = self.db.get_members_batch(
                limit=min(batch_size, daily_target - self.invites_sent_today),
                priority=True
            )
            
            if not members:
                logger.info("No more members to invite")
                break
            
            logger.info(f"🔄 Processing batch {batch_number} ({len(members)} members)")
            
            # Process setiap member dalam batch
            successful_in_batch = 0
            failed_in_batch = 0
            
            for member in members:
                if not self.is_running:
                    break
                
                # Cek daily limit
                if self.invites_sent_today >= daily_target:
                    break
                
                # Cek hourly limit
                if self._check_hourly_limit():
                    logger.warning("⚠️ Hourly limit reached. Taking a break...")
                    await asyncio.sleep(3600)  # Tunggu 1 jam
                    self.last_reset_time = datetime.now()
                
                # Invite member
                success = await self._invite_single_member(
                    target_entity, 
                    member
                )
                
                if success:
                    successful_in_batch += 1
                    self.invites_sent_today += 1
                    self.total_invites_sent += 1
                else:
                    failed_in_batch += 1
                
                # Update progress setiap 10 invites
                if (successful_in_batch + failed_in_batch) % 10 == 0:
                    logger.info(f"  ↪ Progress: {self.invites_sent_today}/{daily_target}")
                
                # Random delay antara invites
                delay = random.uniform(*self.config.delay_between_invites)
                await asyncio.sleep(delay)
            
            # Update database progress
            self.db.update_daily_progress(successful_in_batch, failed_in_batch)
            
            logger.info(f"✅ Batch {batch_number} completed: "
                       f"{successful_in_batch} successful, {failed_in_batch} failed")
            
            # Break setelah batch (jika config aktif)
            if (batch_number * batch_size) % self.config.break_after_batch == 0:
                break_duration = random.randint(*self.config.break_duration)
                logger.info(f"⏸️ Taking break: {break_duration} seconds")
                await asyncio.sleep(break_duration)
            
            batch_number += 1
        
        logger.info(f"✅ Day {day} completed: "
                   f"{self.invites_sent_today} invites sent")
    
    async def _invite_single_member(self, target_entity, member: Dict) -> bool:
        """Invite single member ke grup target"""
        try:
            # Dapatkan user entity
            user_entity = await self.client.get_entity(member['user_id'])
            
            # Invite ke channel
            await self.client(InviteToChannelRequest(
                channel=target_entity,
                users=[user_entity]
            ))
            
            # Update database
            self.db.update_member_status(member['user_id'], success=True)
            
            logger.debug(f"✅ Invited: {member['first_name']} "
                        f"(@{member.get('username', 'N/A')})")
            
            return True
            
        except errors.UserAlreadyParticipantError:
            self.db.update_member_status(
                member['user_id'], 
                success=True, 
                error_msg="Already a member"
            )
            logger.debug(f"ℹ️ Already member: {member['first_name']}")
            return True
            
        except errors.UserPrivacyRestrictedError:
            self.db.update_member_status(
                member['user_id'], 
                success=False, 
                error_msg="Privacy restricted"
            )
            logger.debug(f"🔒 Privacy restricted: {member['first_name']}")
            return False
            
        except errors.FloodWaitError as e:
            logger.warning(f"⚠️ FloodWait: {e.seconds} seconds")
            await asyncio.sleep(e.seconds + 5)
            return False
            
        except Exception as e:
            error_msg = str(e)[:100]  # Potong error message
            self.db.update_member_status(
                member['user_id'], 
                success=False, 
                error_msg=error_msg
            )
            logger.debug(f"❌ Failed: {member['first_name']} - {error_msg}")
            return False
    
    def _check_hourly_limit(self) -> bool:
        """Cek apakah hourly limit tercapai"""
        time_since_reset = datetime.now() - self.last_reset_time
        if time_since_reset.total_seconds() > 3600:  # 1 jam
            self.last_reset_time = datetime.now()
            return False
        
        hourly_invites = self.invites_sent_today
        return hourly_invites >= self.config.max_hourly_invites
    
    async def _wait_until_tomorrow(self):
        """Tunggu sampai besok (24 jam dari start hari ini)"""
        wait_seconds = 24 * 3600  # 24 jam dalam detik
        
        # Kurangi waktu yang sudah dipakai hari ini
        time_used = (datetime.now() - self.last_reset_time).total_seconds()
        wait_seconds = max(3600, wait_seconds - time_used)  # Minimal 1 jam
        
        logger.info(f"⏳ Waiting {wait_seconds/3600:.1f} hours until tomorrow...")
        
        # Progress indicator
        hours = int(wait_seconds // 3600)
        minutes = int((wait_seconds % 3600) // 60)
        
        for i in range(int(wait_seconds // 60)):  # Update setiap menit
            if not self.is_running:
                break
            await asyncio.sleep(60)
            
            # Log progress setiap 30 menit
            if i % 30 == 0:
                remaining = wait_seconds - (i * 60)
                hours_rem = int(remaining // 3600)
                mins_rem = int((remaining % 3600) // 60)
                logger.info(f"⏰ Resume in: {hours_rem}h {mins_rem}m")
    
    async def _complete_migration(self):
        """Selesaikan migrasi dan generate report"""
        logger.info("\n" + "="*60)
        logger.info("🎉 MIGRATION COMPLETED!")
        logger.info("="*60)
        
        # Generate final report
        await self._generate_final_report()
        
        # Print summary
        stats = self.db.get_stats()
        duration = datetime.now() - self.start_time
        
        logger.info(f"📊 FINAL SUMMARY:")
        logger.info(f"  ⏱️  Total duration: {duration.days} days, "
                   f"{duration.seconds//3600} hours")
        logger.info(f"  ✅ Total invited: {stats.invited_total}")
        logger.info(f"  ❌ Total failed: {stats.failed_total}")
        logger.info(f"  📈 Success rate: {stats.success_rate:.2f}%")
        logger.info(f"  🎯 Target achieved: "
                   f"{(stats.invited_total/self.config.total_members*100):.1f}%")
        
        # Save final state
        self.is_running = False
    
    async def _generate_extraction_report(self):
        """Generate report setelah extraction"""
        stats = self.db.get_stats()
        
        report = {
            'extraction_date': datetime.now().isoformat(),
            'source_group': self.config.source_group,
            'total_extracted': stats.total_members,
            'status_distribution': self._get_status_distribution(),
            'priority_distribution': self._get_priority_distribution(),
            'extraction_duration': str(datetime.now() - self.start_time)
        }
        
        # Save to JSON
        with open('extraction_report.json', 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        logger.info("📄 Extraction report saved: extraction_report.json")
    
    async def _generate_final_report(self):
        """Generate final report Excel"""
        with sqlite3.connect(self.db.db_path) as conn:
            # Read semua data
            members_df = pd.read_sql_query('SELECT * FROM members', conn)
            daily_df = pd.read_sql_query('SELECT * FROM daily_progress', conn)
            errors_df = pd.read_sql_query('SELECT * FROM error_logs', conn)
            
            # Create Excel writer
            with pd.ExcelWriter('migration_final_report.xlsx', engine='openpyxl') as writer:
                # Sheet 1: Summary
                summary_data = {
                    'Metric': [
                        'Total Members', 'Successfully Invited', 
                        'Failed Invites', 'Success Rate', 'Migration Duration'
                    ],
                    'Value': [
                        len(members_df),
                        len(members_df[members_df['invited'] == 1]),
                        len(members_df[members_df['attempts'] >= 3]),
                        f"{(len(members_df[members_df['invited'] == 1])/len(members_df)*100):.2f}%",
                        str(datetime.now() - self.start_time)
                    ]
                }
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
                
                # Sheet 2: All Members
                members_df.to_excel(writer, sheet_name='All Members', index=False)
                
                # Sheet 3: Daily Progress
                daily_df.to_excel(writer, sheet_name='Daily Progress', index=False)
                
                # Sheet 4: Error Analysis
                errors_df.to_excel(writer, sheet_name='Errors', index=False)
                
                # Sheet 5: Statistics by Status
                status_stats = members_df.groupby('status').agg({
                    'user_id': 'count',
                    'invited': 'sum',
                    'attempts': 'mean'
                }).round(2)
                status_stats.to_excel(writer, sheet_name='Status Stats')
        
        logger.info("📊 Final report saved: migration_final_report.xlsx")
    
    def _get_status_distribution(self):
        """Dapatkan distribusi status member"""
        with sqlite3.connect(self.db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT status, COUNT(*) as count
                FROM members
                GROUP BY status
                ORDER BY 
                    CASE status
                        WHEN 'recently' THEN 1
                        WHEN 'last_week' THEN 2
                        WHEN 'last_month' THEN 3
                        ELSE 4
                    END
            ''')
            return dict(cursor.fetchall())
    
    def _get_priority_distribution(self):
        """Dapatkan distribusi priority"""
        with sqlite3.connect(self.db.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT priority, COUNT(*) as count
                FROM members
                GROUP BY priority
                ORDER BY priority
            ''')
            return dict(cursor.fetchall())
    
    def stop_migration(self):
        """Stop migrasi dengan aman"""
        logger.info("🛑 Stopping migration...")
        self.is_running = False
    
    async def get_realtime_stats(self) -> Dict:
        """Dapatkan statistik realtime"""
        stats = self.db.get_stats()
        
        return {
            'is_running': self.is_running,
            'current_day': self.current_day,
            'total_days': self.config.days_to_complete,
            'invites_sent_today': self.invites_sent_today,
            'daily_target': self.config.max_daily_invites,
            'total_invites_sent': self.total_invites_sent,
            'stats': asdict(stats),
            'estimated_completion': stats.estimated_completion.isoformat() 
                if stats.estimated_completion else None,
            'elapsed_time': str(datetime.now() - self.start_time) 
                if self.start_time else None
        }

# ===== MAIN EXECUTION =====
async def main():
    """Fungsi utama untuk menjalankan migrasi"""
    
    # ===== KONFIGURASI =====
    API_ID = 25092524  # GANTI DENGAN API ID ANDA
    API_HASH = '7b14928f710a3992e87e665be40fa6c0'  # GANTI DENGAN API HASH ANDA
    
    # Grup sumber (yang memiliki 50K member)
    SOURCE_GROUP = '@tongkrongan_fwb'  # atau link/ID
    
    # Grup target (grup baru)
    TARGET_GROUP = '@artemis_pretty'  # atau link/ID
    
    # ===== SETUP CONFIG =====
    config = MigrationConfig(
        source_group=SOURCE_GROUP,
        target_group=TARGET_GROUP,
        total_members=50000,
        days_to_complete=7,
        max_daily_invites=400,
        max_hourly_invites=80,
        delay_between_invites=(3.0, 8.0),
        break_after_batch=50,
        break_duration=(30, 60)
    )
    
    # ===== INISIALISASI =====
    logger.info("🚀 Initializing 50K Member Migration System")
    migrator = MassMigrationEngine(API_ID, API_HASH, config)
    
    try:
        # Step 1: Connect ke Telegram
        logger.info("Step 1: Connecting to Telegram...")
        if not await migrator.connect():
            logger.error("Failed to connect. Exiting.")
            return
        
        # Step 2: Extract members dari grup lama
        logger.info("\nStep 2: Extracting members from source group...")
        extracted = await migrator.extract_all_members()
        
        if extracted == 0:
            logger.error("No members extracted. Exiting.")
            return
        
        # Step 3: Konfirmasi sebelum mulai migrasi
        logger.info("\n" + "="*60)
        logger.info(f"READY TO START MIGRATION")
        logger.info(f"Members extracted: {extracted}")
        logger.info(f"Target group: {TARGET_GROUP}")
        logger.info(f"Estimated time: {config.days_to_complete} days")
        logger.info("="*60)
        
        # Tunggu konfirmasi (optional)
        # input("Press Enter to start migration...")
        
        # Step 4: Start migration
        logger.info("\nStep 3: Starting migration process...")
        await migrator.start_migration()
        
    except KeyboardInterrupt:
        logger.info("\n⚠️ Migration interrupted by user")
        migrator.stop_migration()
        
    except Exception as e:
        logger.error(f"❌ Critical error: {e}")
        
    finally:
        # Cleanup
        if migrator.client:
            await migrator.client.disconnect()
            logger.info("Disconnected from Telegram")
        
        logger.info("Migration system shutdown complete")

# ===== RUN SCRIPT =====
if __name__ == '__main__':
    # Create necessary directories
    import os
    os.makedirs('reports', exist_ok=True)
    os.makedirs('backups', exist_ok=True)
    
    # Run migration
    asyncio.run(main())
