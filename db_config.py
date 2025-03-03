from datetime import datetime
import sqlalchemy as sql
from sqlalchemy import MetaData, Table, Column, Integer, DateTime, DECIMAL, VARCHAR, TEXT, Index, UniqueConstraint, \
    func, BOOLEAN, create_engine, Date, ForeignKey, Enum, Time, Float, text, String, BigInteger, Boolean
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP

use_sqlite = False
rdbms_type = "postgres"

db_name = f"NOTIS_API"
pg_user = "postgres"
pg_pass = "postgres"
pg_host = "192.168.112.219"
pg_port = "5432"

# db_name = f"data_arathi"
# pg_user = "postgres"
# pg_pass = "root"
# pg_host = "172.16.47.81"
# pg_port = "5432"

engine_str = f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{db_name}"

metadata = MetaData()

n_tbl_notis_trade_book = "NOTIS_TRADE_BOOK"
s_tbl_notis_trade_book = Table(
    n_tbl_notis_trade_book, metadata,
    Column("ID", BigInteger),
    Column("seqNo", BigInteger),
    Column("mkt", BigInteger),
    Column("trdNo", BigInteger),
    Column("trdTm", String(50)),
    Column("Tkn", BigInteger),
    Column("trdQty", BigInteger),
    Column("trdPrc", BigInteger),
    Column("bsFlg", String(50)),
    Column("ordNo", BigInteger),
    Column("brnCd", BigInteger),
    Column("usrId", BigInteger),
    Column("proCli", BigInteger),
    Column("cliActNo", String(50)),
    Column("cpCD", String(50)),
    Column("remarks", String(50), nullable=True),  # Assuming NULL values would be represented as empty String(50)s in absence of nullable clause
    Column("actTyp", BigInteger),
    Column("TCd", BigInteger),
    Column("ordTm", String(50)),
    Column("Booktype", BigInteger),
    Column("oppTmCd", String(50), nullable=True),  # Changed NoneType to String(50) to handle nullable scenario
    Column("ctclid", BigInteger),
    Column("status", String(50)),
    Column("TmCd", BigInteger),
    Column("sym", String(50)),
    Column("ser", String(50), nullable=True),  # Changed NoneType to String(50)
    Column("inst", String(50)),
    Column("expDt", String(50)),
    Column("strPrc", BigInteger),
    Column("optType", String(50)),
    Column("sessionID", String(50), nullable=True),  # Changed NoneType to String(50)
    Column("echoback", String(50), nullable=True),  # Changed NoneType to String(50)
    Column("Fill1", String(50), nullable=True),  # Changed NoneType to String(50)
    Column("Fill2", String(50), nullable=True),
    Column("Fill3", String(50), nullable=True),
    Column("Fill4", String(50), nullable=True),
    Column("Fill5", String(50), nullable=True),
    Column("Fill6", String(50), nullable=True),
    Column("Column38", String(50)),
    Column("messageId", BigInteger),
    Column("CreateDate", DateTime),
    Column("TerminalID", String(50)),
    Column("TerminalName", String(50)),
    Column("UserID", String(50)),
    Column("SubGroup", String(50)),
    Column("MainGroup", String(50)),
    Column("NeatID", String(50))
)

n_tbl_notis_raw_data = "notis_raw_data"
s_tbl_notis_raw_data = Table(
    n_tbl_notis_raw_data, metadata,
    Column("ID", BigInteger),
    Column("Column1", BigInteger),
    Column("Column2", BigInteger),
    Column("Column3", BigInteger),
    Column("Column4", String(50)),
    Column("Column5", BigInteger),
    Column("Column6", BigInteger),
    Column("Column7", BigInteger),
    Column("Column8", String(50)),
    Column("Column9", BigInteger),
    Column("Column10", BigInteger),
    Column("Column11", BigInteger),
    Column("Column12", BigInteger),
    Column("Column13", String(50)),
    Column("Column14", String(50)),
    Column("Column15", String(50), nullable=True),
    Column("Column16", BigInteger),
    Column("Column17", BigInteger),
    Column("Column18", String(50)),
    Column("Column19", BigInteger),
    Column("Column20", String(50), nullable=True),
    Column("Column21", BigInteger),
    Column("Column22", String(50)),
    Column("Column23", BigInteger),
    Column("Column24", String(50)),
    Column("Column25", String(50), nullable=True),
    Column("Column26", String(50)),
    Column("Column27", String(50)),
    Column("Column28", BigInteger),
    Column("Column29", String(50)),
    Column("Column30", String(50), nullable=True),
    Column("Column31", String(50), nullable=True),
    Column("Column32", String(50), nullable=True),
    Column("Column33", String(50), nullable=True),
    Column("Column34", String(50), nullable=True),
    Column("Column35", String(50), nullable=True),
    Column("Column36", String(50)),
    Column("Column37", BigInteger),
    Column("Column38", String(50)),
    Column("messageId", BigInteger),
    Column("CreateDate", DateTime)
)

n_tbl_notis_nnf_data = "nnf_data"
s_tbl_notis_nnf_data = Table(
    n_tbl_notis_nnf_data, metadata,
    Column("NNFID", BigInteger),
    Column("TerminalID", String(50)),
    Column("TerminalName", String(50)),
    Column("UserID", String(50)),
    Column("SubGroup", String(50)),
    Column("MainGroup", String(50)),
    Column("NeatID", BigInteger)
)

# # n_tbl_notis_desk_wise_final_net_position = f"NOTIS_DESK_WISE_FINAL_NET_POSITION_{datetime(year=2025, month=1, day=8).date().strftime('%Y-%m-%d')}"
# n_tbl_notis_datewise_net_position = f"NOTIS_NET_POSITION_{datetime.now().date().strftime('%Y-%m-%d')}"
# s_tbl_notis_datewise_net_position = Table(
#     n_tbl_notis_datewise_net_position, metadata,
#     Column("Underlying", String(50)),
#     Column("Strike", BigInteger),
#     Column("OptionType", String(2)),
#     Column("Expiry", String(50)),
#     Column("Long", BigInteger, nullable=True),
#     Column("Short", BigInteger, nullable=True),
#     Column("ClosingQty", BigInteger, nullable=True),
#     Column("ClosingPrice", BigInteger),
#     Column("SubGroup", String(50)),
#     Column("MainGroup", String(50)),
# )

n_tbl_notis_desk_wise_net_position = f"NOTIS_DESK_WISE_EOD_POSITION_{datetime.now().date().strftime('%Y-%m-%d')}"
s_tbl_notis_desk_wise_net_position = Table(
    n_tbl_notis_desk_wise_net_position, metadata,
    Column("EodUnderlying", String(50)),
    Column("EodStrike", Float),
    Column("EodOptionType", String(10)),
    Column("EodExpiry", String(50)),
    Column("EodLong", BigInteger),
    Column("EodShort", BigInteger),
    Column("EodClosingQty", BigInteger),
    Column("EodClosingPrice", Float),
    Column("EodSubGroup", String(50)),
    Column("EodMainGroup", String(50)),
    Column("BuyQty", BigInteger),
    Column("buyAvgPrice", Float),
    Column("SellQty", BigInteger),
    Column("sellAvgPrice", Float),
    Column("volume", BigInteger),
    Column("ClosingQty", BigInteger),
    Column("ClosingPrice", Float),
    Column("Long", BigInteger),
    Column("Short", BigInteger),
    Column("IntradayPnL", Float, nullable=True),
    Column("expired", Boolean, nullable=True),
    Column("Spot", Float, nullable=True),
    Column("ExpRate", Float, nullable=True),
    Column("ExpBuyQty", BigInteger, nullable=True),
    Column("ExpBuyRate", Float, nullable=True),
    Column("ExpSellQty", BigInteger, nullable=True),
    Column("ExpSellRate", Float, nullable=True),
    Column("ExpBuyValue", Float, nullable=True),
    Column("ExpSellValue", Float, nullable=True)
)

# n_tbl_notis_net_position = f"NOTIS_NET_POSITION"
# s_tbl_notis_net_position = Table(
#     n_tbl_notis_net_position, metadata,
#     Column("EodUnderlying", String(50)),
#     Column("EodStrike", BigInteger),
#     Column("EodOptionType", String(2)),
#     Column("EodExpiry", String(50)),
#     Column("EodLong", BigInteger, nullable=True),
#     Column("EodShort", BigInteger, nullable=True),
#     Column("EodClosingQty", BigInteger, nullable=True),
#     Column("EodClosingPrice", BigInteger, nullable=True),
#     Column("EodSubGroup", String(50)),
#     Column("EodMainGroup", String(50)),
#     Column("BuyQty", BigInteger, nullable=True),
#     Column("SellQty", BigInteger, nullable=True),
#     Column("volume", BigInteger, nullable=True),
#     Column("ClosingQty", BigInteger, nullable=True),
#     Column("ClosingPrice", BigInteger)
# )

# Last and after all table declarations
# noinspection PyUnboundLocalVariable
meta_engine = sql.create_engine(engine_str)
metadata.create_all(meta_engine)
meta_engine.dispose()
