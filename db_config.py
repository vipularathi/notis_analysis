from datetime import datetime
import sqlalchemy as sql
from sqlalchemy import MetaData, Table, Column, Integer, DateTime, DECIMAL, VARCHAR, TEXT, Index, UniqueConstraint, \
    func, BOOLEAN, create_engine, Date, ForeignKey, Enum, Time, Float, text, String, BigInteger
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP

use_sqlite = False
rdbms_type = 'postgres'

db_name = f'NOTIS_API'
pg_user = 'postgres'
pg_pass = 'postgres'
pg_host = '192.168.112.219'
pg_port = '5432'

engine_str = f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{db_name}"

metadata = MetaData()

n_tbl_notis_trade_book = "notis_trade_book"
s_tbl_notis_trade_book = Table(
    n_tbl_notis_trade_book, metadata,
    Column('ID', BigInteger),
    Column('seqNo', BigInteger),
    Column('mkt', BigInteger),
    Column('trdNo', BigInteger),
    Column('trdTm', String(50)),
    Column('Tkn', BigInteger),
    Column('trdQty', BigInteger),
    Column('trdPrc', BigInteger),
    Column('bsFlg', String(50)),
    Column('ordNo', BigInteger),
    Column('brnCd', BigInteger),
    Column('usrId', BigInteger),
    Column('proCli', BigInteger),
    Column('cliActNo', String(50)),
    Column('cpCd', String(50)),
    Column('remarks', String(50), nullable=True),  # Assuming NULL values would be represented as empty String(50)s in absence of nullable clause
    Column('actTyp', BigInteger),
    Column('TCd', BigInteger),
    Column('ordTm', String(50)),
    Column('Booktype', BigInteger),
    Column('oppTmCd', String(50), nullable=True),  # Changed NoneType to String(50) to handle nullable scenario
    Column('ctclid', BigInteger),
    Column('status', String(50)),
    Column('TmCd', BigInteger),
    Column('sym', String(50)),
    Column('ser', String(50), nullable=True),  # Changed NoneType to String(50)
    Column('inst', String(50)),
    Column('expDt', String(50)),
    Column('strPrc', BigInteger),
    Column('optType', String(50)),
    Column('sessionID', String(50), nullable=True),  # Changed NoneType to String(50)
    Column('echoback', String(50), nullable=True),  # Changed NoneType to String(50)
    Column('Fill1', String(50), nullable=True),  # Changed NoneType to String(50)
    Column('Fill2', String(50), nullable=True),
    Column('Fill3', String(50), nullable=True),
    Column('Fill4', String(50), nullable=True),
    Column('Fill5', String(50), nullable=True),
    Column('Fill6', String(50), nullable=True),
    Column('Column38', String(50)),
    Column('messageId', BigInteger),
    Column('CreateDate', DateTime),
    Column('TerminalID', String(50)),
    Column('TerminalName', String(50)),
    Column('UserID', String(50)),
    Column('SubGroup', String(50)),
    Column('MainGroup', String(50)),
    Column('NeatID', String(50))
)

# Last and after all table declarations
# noinspection PyUnboundLocalVariable
meta_engine = sql.create_engine(engine_str)
metadata.create_all(meta_engine)
meta_engine.dispose()
