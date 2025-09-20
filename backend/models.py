from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, BigInteger, Date, Text, Float, TIMESTAMP, text

Base = declarative_base()

class AirQualityRaw(Base):
    __tablename__ = "air_quality_raw"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    date_local = Column(Date, nullable=False)
    parameter_name = Column(Text, nullable=False)
    arithmetic_mean = Column(Float, nullable=False)
    local_site_name = Column(Text)
    state_name = Column(Text, nullable=False)
    county_name = Column(Text)
    city_name = Column(Text)
    cbsa_name = Column(Text)
    created_at = Column(TIMESTAMP(timezone=True), server_default=text("NOW()"), nullable=False)
