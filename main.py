"""
File: mass_migration_fixed.py
SISTEM MIGRASI 50K MEMBER - FIXED DATABASE SYNTAX
"""
import os  # TAMBAHKAN INI
import asyncio
import time
import random
import sqlite3
import json
import sys  # TAMBAHKAN INI
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import logging
from dataclasses import dataclass
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

# ===== DATABASE MANAGER YANG DIPERBAIKI =====
class MigrationDatabase:
    """Manager database SQLite untuk migrasi - FIXED SYNTAX"""
    
    def __init__(self, db_path: str = 'migration_50k.db'):
        self.db_path = db_path
        self.init_database()
        self.create_indexes()
    
    def init_database(self):
        """Initialize database schema dengan syntax yang benar"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS members (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER UNIQUE,
                    username TEXT,
                    phone TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    is_bot INTEGER DEFAULT 0,
                    status TEXT,
                    last_seen TIMESTAMP,
                    extracted_at TIMESTAMP,
                    priority INTEGER DEFAULT 5,
                    invited INTEGER DEFAULT 0,
                    invited_at TIMESTAMP NULL,
                    attempts INTEGER DEFAULT 0,
                    error_message TEXT NULL
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS daily_progress (
                    date DATE PRIMARY KEY,
                    invited_count INTEGER DEFAULT 0,
                    failed_count INTEGER DEFAULT 0,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP NULL
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS error_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    error_type TEXT,
                    error_message TEXT,
                    occurred_at TIMESTAMP,
                    resolved INTEGER DEFAULT 0
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS system_config (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    updated_at TIMESTAMP
                )
            ''')
            
            conn.commit()
            logger.info("✅ Database tables created successfully")
    
    def create_indexes(self):
        """Create indexes terpisah"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            indexes = [
                'idx_members_priority',
                'idx_members_invited',
                'idx_members_status',
                'idx_members_user_id',
                'idx_error_logs_user_id'
            ]
            
            for idx in indexes:
                try:
                    cursor.execute(f'DROP INDEX IF EXISTS {idx}')
                except:
                    pass
            
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_members_priority ON members (priority)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_members_invited ON members (invited)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_members_status ON members (status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_members_user_id ON members (user_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_error_logs_user_id ON error_logs (user_id)')
            
            conn.commit()
            logger.info("✅ Database indexes created successfully")
    
    def save_member(self, member_data: Dict) -> bool:
        """Save member ke database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute('SELECT user_id FROM members WHERE user_id = ?', 
                             (member_data['user_id'],))
                existing = cursor.fetchone()
                
                if existing:
                    cursor.execute('''
                        UPDATE members 
                        SET username = ?, phone = ?, first_name = ?, last_name = ?,
                            is_bot = ?, status = ?, last_seen = ?, priority = ?,
                            extracted_at = ?
                        WHERE user_id = ?
                    ''', (
                        member_data.get('username'),
                        member_data.get('phone'),
                        member_data.get('first_name', 'Unknown'),
                        member_data.get('last_name', ''),
                        member_data.get('is_bot', 0),
                        member_data.get('status', 'unknown'),
                        member_data.get('last_seen'),
                        member_data.get('priority', 5),
                        datetime.now(),
                        member_data['user_id']
                    ))
                else:
                    cursor.execute('''
                        INSERT INTO members 
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
            logger.error(f"❌ Error saving member: {e}")
            return False
    
    def get_members_batch(self, limit: int = 100, priority: bool = True) -> List[Dict]:
        """Ambil batch members untuk diinvite"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                if priority:
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
                
                results = [dict(row) for row in cursor.fetchall()]
                return results
                
        except Exception as e:
            logger.error(f"❌ Error getting batch: {e}")
            return []
    
    def update_member_status(self, user_id: int, success: bool, error_msg: str = None):
        """Update status member setelah invite attempt"""
        try:
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
                
                if error_msg and not success:
                    cursor.execute('''
                        INSERT INTO error_logs 
                        (user_id, error_type, error_message, occurred_at)
                        VALUES (?, ?, ?, ?)
                    ''', (user_id, 'invite_failed', error_msg, datetime.now()))
                
                conn.commit()
                
        except Exception as e:
            logger.error(f"❌ Error updating member status: {e}")
    
    def get_stats(self) -> Dict:
        """Dapatkan statistik migrasi"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute('SELECT COUNT(*) FROM members')
                total_members = cursor.fetchone()[0]
                
                cursor.execute('SELECT COUNT(*) FROM members WHERE invited = 1')
                invited_total = cursor.fetchone()[0]
                
                today = datetime.now().strftime('%Y-%m-%d')
                cursor.execute('''
                    SELECT invited_count, failed_count 
                    FROM daily_progress 
                    WHERE date = ?
                ''', (today,))
                result = cursor.fetchone()
                invited_today = result[0] if result else 0
                failed_today = result[1] if result else 0
                
                cursor.execute('SELECT COUNT(*) FROM members WHERE attempts >= 3 AND invited = 0')
                failed_total = cursor.fetchone()[0]
                
                cursor.execute('SELECT status, COUNT(*) FROM members GROUP BY status')
                status_dist = dict(cursor.fetchall())
                
                remaining = total_members - invited_total
                if invited_today > 0:
                    days_remaining = remaining / invited_today
                    estimated = datetime.now() + timedelta(days=days_remaining)
                    estimated_str = estimated.strftime('%Y-%m-%d %H:%M')
                else:
                    estimated_str = None
                
                success_rate = (invited_total / total_members * 100) if total_members > 0 else 0
                
                return {
                    'total_members': total_members,
                    'extracted_members': total_members,
                    'invited_today': invited_today,
                    'invited_total': invited_total,
                    'failed_today': failed_today,
                    'failed_total': failed_total,
                    'remaining_members': remaining,
                    'estimated_completion': estimated_str,
                    'success_rate': round(success_rate, 2),
                    'status_distribution': status_dist
                }
                
        except Exception as e:
            logger.error(f"❌ Error getting stats: {e}")
            return {}
    
    def start_daily_session(self):
        """Start session harian"""
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR IGNORE INTO daily_progress (date, start_time)
                    VALUES (?, ?)
                ''', (today, datetime.now()))
                conn.commit()
                logger.debug(f"✅ Daily session started for {today}")
                
        except Exception as e:
            logger.error(f"❌ Error starting daily session: {e}")
    
    def update_daily_progress(self, invited: int = 0, failed: int = 0):
        """Update progress harian"""
        try:
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
                
        except Exception as e:
            logger.error(f"❌ Error updating daily progress: {e}")

# ===== MIGRATION ENGINE =====
class MassMigrationEngine:
    """Engine utama untuk migrasi massal"""
    
    def __init__(self, api_id: int, api_hash: str, config: MigrationConfig):
        self.api_id = api_id
        self.api_hash = api_hash
        self.config = config
        self.client = None
        self.db = MigrationDatabase()
        
        self.is_running = False
        self.current_day = 1
        self.invites_sent_today = 0
        self.last_reset_time = datetime.now()
        
        self.start_time = None
        self.total_invites_sent = 0
        
        logger.info("🚀 MassMigrationEngine initialized")
    
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
        """Extract SEMUA member dari grup sumber"""
        logger.info(f"🚀 Starting extraction from: {self.config.source_group}")
        
        try:
            source_entity = await self.client.get_entity(self.config.source_group)
            
            full_channel = await self.client(GetFullChannelRequest(source_entity))
            total_participants = getattr(full_channel.full_chat, 'participants_count', 0)
            logger.info(f"📊 Total participants: {total_participants}")
            
            extracted_count = 0
            offset = 0
            limit = 200
            batch_number = 1
            
            while True:
                try:
                    logger.info(f"📦 Processing batch {batch_number}...")
                    
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
                    
                    for user in participants.users:
                        status = self._determine_user_status(user)
                        priority = self._calculate_priority(status)
                        
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
                        
                        if extracted_count % 1000 == 0:
                            logger.info(f"✅ Extracted: {extracted_count}/{total_participants}")
                    
                    offset += len(participants.users)
                    batch_number += 1
                    
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
        
        self.db.start_daily_session()
        
        day = 1
        while day <= self.config.days_to_complete and self.is_running:
            logger.info(f"\n{'='*60}")
            logger.info(f"📅 DAY {day}/{self.config.days_to_complete}")
            logger.info(f"{'='*60}")
            
            self.invites_sent_today = 0
            self.last_reset_time = datetime.now()
            
            await self._migrate_day(day)
            
            stats = self.db.get_stats()
            logger.info(f"📊 Day {day} Summary:")
            logger.info(f"  ✅ Invited today: {stats.get('invited_today', 0)}")
            logger.info(f"  ❌ Failed today: {stats.get('failed_today', 0)}")
            logger.info(f"  📈 Total invited: {stats.get('invited_total', 0)}/{stats.get('total_members', 0)}")
            logger.info(f"  ⏳ Remaining: {stats.get('remaining_members', 0)}")
            
            if stats.get('remaining_members', 0) <= 0:
                logger.info("🎉 All members have been invited!")
                break
            
            if day < self.config.days_to_complete:
                logger.info("⏸️ Pausing until tomorrow...")
                await self._wait_until_tomorrow()
            
            day += 1
        
        await self._complete_migration()
    
    async def _migrate_day(self, day: int):
        """Eksekusi migrasi untuk satu hari"""
        try:
            target_entity = await self.client.get_entity(self.config.target_group)
            
            stats = self.db.get_stats()
            daily_target = min(
                self.config.max_daily_invites,
                stats.get('remaining_members', 0)
            )
            
            logger.info(f"🎯 Daily target: {daily_target} invites")
            
            batch_size = 50
            batch_number = 1
            
            while self.invites_sent_today < daily_target and self.is_running:
                members = self.db.get_members_batch(
                    limit=min(batch_size, daily_target - self.invites_sent_today),
                    priority=True
                )
                
                if not members:
                    logger.info("No more members to invite")
                    break
                
                logger.info(f"🔄 Processing batch {batch_number} ({len(members)} members)")
                
                successful_in_batch = 0
                failed_in_batch = 0
                
                for member in members:
                    if not self.is_running:
                        break
                    
                    if self.invites_sent_today >= daily_target:
                        break
                    
                    if self._check_hourly_limit():
                        logger.warning("⚠️ Hourly limit reached. Taking a break...")
                        await asyncio.sleep(3600)
                        self.last_reset_time = datetime.now()
                    
                    success = await self._invite_single_member(target_entity, member)
                    
                    if success:
                        successful_in_batch += 1
                        self.invites_sent_today += 1
                        self.total_invites_sent += 1
                    else:
                        failed_in_batch += 1
                    
                    if (successful_in_batch + failed_in_batch) % 10 == 0:
                        logger.info(f"  ↪ Progress: {self.invites_sent_today}/{daily_target}")
                    
                    delay = random.uniform(*self.config.delay_between_invites)
                    await asyncio.sleep(delay)
                
                self.db.update_daily_progress(successful_in_batch, failed_in_batch)
                
                logger.info(f"✅ Batch {batch_number} completed: "
                           f"{successful_in_batch} successful, {failed_in_batch} failed")
                
                if (batch_number * batch_size) % self.config.break_after_batch == 0:
                    break_duration = random.randint(*self.config.break_duration)
                    logger.info(f"⏸️ Taking break: {break_duration} seconds")
                    await asyncio.sleep(break_duration)
                
                batch_number += 1
            
            logger.info(f"✅ Day {day} completed: {self.invites_sent_today} invites sent")
            
        except Exception as e:
            logger.error(f"❌ Error in day {day} migration: {e}")
    
    async def _invite_single_member(self, target_entity, member: Dict) -> bool:
        """Invite single member ke grup target"""
        try:
            user_entity = await self.client.get_entity(member['user_id'])
            
            await self.client(InviteToChannelRequest(
                channel=target_entity,
                users=[user_entity]
            ))
            
            self.db.update_member_status(member['user_id'], success=True)
            
            logger.debug(f"✅ Invited: {member['first_name']} (@{member.get('username', 'N/A')})")
            
            return True
            
        except errors.UserAlreadyParticipantError:
            self.db.update_member_status(member['user_id'], success=True, error_msg="Already a member")
            logger.debug(f"ℹ️ Already member: {member['first_name']}")
            return True
            
        except errors.UserPrivacyRestrictedError:
            self.db.update_member_status(member['user_id'], success=False, error_msg="Privacy restricted")
            logger.debug(f"🔒 Privacy restricted: {member['first_name']}")
            return False
            
        except errors.FloodWaitError as e:
            logger.warning(f"⚠️ FloodWait: {e.seconds} seconds")
            await asyncio.sleep(e.seconds + 5)
            return False
            
        except Exception as e:
            error_msg = str(e)[:100]
            self.db.update_member_status(member['user_id'], success=False, error_msg=error_msg)
            logger.debug(f"❌ Failed: {member['first_name']} - {error_msg}")
            return False
    
    def _check_hourly_limit(self) -> bool:
        """Cek apakah hourly limit tercapai"""
        time_since_reset = datetime.now() - self.last_reset_time
        if time_since_reset.total_seconds() > 3600:
            self.last_reset_time = datetime.now()
            return False
        
        hourly_invites = self.invites_sent_today
        return hourly_invites >= self.config.max_hourly_invites
    
    async def _wait_until_tomorrow(self):
        """Tunggu sampai besok"""
        wait_seconds = 24 * 3600
        
        time_used = (datetime.now() - self.last_reset_time).total_seconds()
        wait_seconds = max(3600, wait_seconds - time_used)
        
        logger.info(f"⏳ Waiting {wait_seconds/3600:.1f} hours until tomorrow...")
        
        hours = int(wait_seconds // 3600)
        minutes = int((wait_seconds % 3600) // 60)
        
        for i in range(int(wait_seconds // 60)):
            if not self.is_running:
                break
            await asyncio.sleep(60)
            
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
        
        await self._generate_final_report()
        
        stats = self.db.get_stats()
        duration = datetime.now() - self.start_time
        
        logger.info(f"📊 FINAL SUMMARY:")
        logger.info(f"  ⏱️  Total duration: {duration.days} days, {duration.seconds//3600} hours")
        logger.info(f"  ✅ Total invited: {stats.get('invited_total', 0)}")
        logger.info(f"  ❌ Total failed: {stats.get('failed_total', 0)}")
        logger.info(f"  📈 Success rate: {stats.get('success_rate', 0)}%")
        logger.info(f"  🎯 Target achieved: {(stats.get('invited_total', 0)/self.config.total_members*100):.1f}%")
        
        self.is_running = False
    
    async def _generate_extraction_report(self):
        """Generate report setelah extraction"""
        stats = self.db.get_stats()
        
        report = {
            'extraction_date': datetime.now().isoformat(),
            'source_group': self.config.source_group,
            'total_extracted': stats.get('total_members', 0),
            'status_distribution': stats.get('status_distribution', {}),
            'extraction_duration': str(datetime.now() - self.start_time)
        }
        
        with open('extraction_report.json', 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        logger.info("📄 Extraction report saved: extraction_report.json")
    
    async def _generate_final_report(self):
        """Generate final report Excel"""
        try:
            with sqlite3.connect(self.db.db_path) as conn:
                members_df = pd.read_sql_query('SELECT * FROM members', conn)
                daily_df = pd.read_sql_query('SELECT * FROM daily_progress', conn)
                errors_df = pd.read_sql_query('SELECT * FROM error_logs', conn)
                
                with pd.ExcelWriter('migration_final_report.xlsx', engine='openpyxl') as writer:
                    summary_data = {
                        'Metric': ['Total Members', 'Successfully Invited', 'Failed Invites', 'Success Rate', 'Migration Duration'],
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
                    
                    members_df.to_excel(writer, sheet_name='All Members', index=False)
                    daily_df.to_excel(writer, sheet_name='Daily Progress', index=False)
                    errors_df.to_excel(writer, sheet_name='Errors', index=False)
                    
                    status_stats = members_df.groupby('status').agg({
                        'user_id': 'count',
                        'invited': 'sum',
                        'attempts': 'mean'
                    }).round(2)
                    status_stats.to_excel(writer, sheet_name='Status Stats')
            
            logger.info("📊 Final report saved: migration_final_report.xlsx")
            
        except Exception as e:
            logger.error(f"❌ Error generating report: {e}")
    
    def stop_migration(self):
        """Stop migrasi dengan aman"""
        logger.info("🛑 Stopping migration...")
        self.is_running = False

# ===== MONITORING UTILITY (HANYA SATU KALI DEFINISI) =====
class MigrationMonitor:
    """Utility untuk monitoring migrasi realtime"""
    
    @staticmethod
    def show_dashboard(db_path: str = 'migration_50k.db'):
        """Tampilkan dashboard monitoring"""
        if not os.path.exists(db_path):
            print("❌ Database not found!")
            return
        
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM members")
            total = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM members WHERE invited = 1")
            invited = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM members WHERE attempts >= 3 AND invited = 0")
            failed = cursor.fetchone()[0]
            
            cursor.execute("SELECT date, invited_count, failed_count FROM daily_progress ORDER BY date DESC LIMIT 7")
            daily_data = cursor.fetchall()
            
            cursor.execute("SELECT error_type, COUNT(*) FROM error_logs GROUP BY error_type ORDER BY COUNT(*) DESC LIMIT 5")
            top_errors = cursor.fetchall()
            
            conn.close()
            
            os.system('cls' if os.name == 'nt' else 'clear')
            print("="*60)
            print("📊 MIGRATION MONITORING DASHBOARD")
            print("="*60)
            print(f"\n📈 OVERALL STATS:")
            print(f"   Total Members: {total:,}")
            print(f"   Invited: {invited:,} ({invited/total*100:.1f}%)")
            print(f"   Failed: {failed:,}")
            print(f"   Remaining: {total - invited:,}")
            
            if total - failed > 0:
                success_rate = invited/(total-failed)*100
                print(f"   Success Rate: {success_rate:.1f}%")
            else:
                print("   Success Rate: 0%")
            
            print(f"\n📅 LAST 7 DAYS:")
            for date, invited_count, failed_count in daily_data:
                print(f"   {date}: ✅ {invited_count:4d} | ❌ {failed_count:3d}")
            
            print(f"\n🚨 TOP ERRORS:")
            for error_type, count in top_errors:
                print(f"   {error_type}: {count}")
            
            print(f"\n⏰ Last update: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("="*60)
            
        except Exception as e:
            print(f"❌ Error loading dashboard: {e}")
    
    @staticmethod
    def get_status():
        """Get current migration status"""
        if not os.path.exists('migration_50k.db'):
            return {"error": "Database not found"}
        
        try:
            conn = sqlite3.connect('migration_50k.db')
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM members")
            total = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM members WHERE invited = 1")
            invited = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM daily_progress WHERE date = DATE('now')")
            today_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT invited_count FROM daily_progress WHERE date = DATE('now')")
            today_invited = cursor.fetchone()
            today_invited = today_invited[0] if today_invited else 0
            
            conn.close()
            
            return {
                "total_members": total,
                "invited": invited,
                "remaining": total - invited,
                "progress_percentage": (invited / total * 100) if total > 0 else 0,
                "today_invited": today_invited,
                "is_active": today_count > 0
            }
            
        except Exception as e:
            return {"error": str(e)}

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
        
        # Step 4: Start migration
        logger.info("\nStep 3: Starting migration process...")
        await migrator.start_migration()
        
    except KeyboardInterrupt:
        logger.info("\n⚠️ Migration interrupted by user")
        migrator.stop_migration()
        
    except Exception as e:
        logger.error(f"❌ Critical error: {e}")
        
    finally:
        if migrator.client:
            await migrator.client.disconnect()
            logger.info("Disconnected from Telegram")
        
        logger.info("Migration system shutdown complete")

# ===== COMMAND LINE INTERFACE =====
def show_help():
    """Show help menu"""
    print("="*60)
    print("📱 TELEGRAM MASS MIGRATION SYSTEM - 50K MEMBERS")
    print("="*60)
    print("\nCommands:")
    print("  python main.py run     - Start migration")
    print("  python main.py monitor - Show dashboard")
    print("  python main.py status  - Check current status")
    print("  python main.py config  - Show configuration")
    print("  python main.py help    - Show this help")
    print("="*60)

def show_config():
    """Show current configuration"""
    print("="*60)
    print("⚙️ CURRENT CONFIGURATION")
    print("="*60)
    
    config = {
        "days_to_complete": 7,
        "max_daily_invites": 400,
        "max_hourly_invites": 80,
        "delay_between_invites": "3-8 seconds",
        "break_after_batch": 50,
        "break_duration": "30-60 seconds"
    }
    
    for key, value in config.items():
        print(f"  {key}: {value}")
    
    print("\n⚠️ IMPORTANT NOTES:")
    print("  1. Never exceed 400 invites per day")
    print("  2. Use random delays between invites")
    print("  3. Monitor for FloodWait errors")
    print("="*60)

# ===== RUN SCRIPT =====
if __name__ == '__main__':
    # Create necessary directories
    os.makedirs('reports', exist_ok=True)
    os.makedirs('backups', exist_ok=True)
    
    # Handle command line arguments
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == 'run':
            print("🚀 Starting migration process...")
            asyncio.run(main())
            
        elif command == 'monitor':
            MigrationMonitor.show_dashboard()
            
        elif command == 'status':
            status = MigrationMonitor.get_status()
            if 'error' in status:
                print(f"❌ {status['error']}")
            else:
                print("="*60)
                print("📊 MIGRATION STATUS")
                print("="*60)
                print(f"Total Members: {status['total_members']:,}")
                print(f"Invited: {status['invited']:,}")
                print(f"Remaining: {status['remaining']:,}")
                print(f"Progress: {status['progress_percentage']:.1f}%")
                print(f"Today's Invites: {status['today_invited']}")
                print(f"Active Today: {'✅ Yes' if status['is_active'] else '❌ No'}")
                print("="*60)
                
        elif command == 'config':
            show_config()
            
        elif command == 'help' or command == '--help' or command == '-h':
            show_help()
            
        else:
            print(f"❌ Unknown command: {command}")
            print("Use 'python main.py help' for available commands")
    else:
        show_help()
