# Creon-Datareader-Mongo
- 대신증권의 HTS인 CREON의 Plus API를 사용하여 주가 데이터를 받아오는 PyQt 기반의 프로그램이며 nestjs와의 연동을 위해서 MongoDB를 선택했습니다.
---
# Configuration
## Enviroment Configuration
- CREON Plus가 32bit 환경에서 지원되기 때문에 32bit Python이 필요하므로 Anaconda 32-bit 환경에서 구동해야합니다.
- [CreonPlus](https://www.creontrade.com/g.ds?m=2194&p=12294&v=11951)에서 크레온 플러스 다운
- [CreonPlusAPI](https://money2.creontrade.com/E5/WTS/Customer/GuideTrading/CW_TradingSystemPlus_Page.aspx?m=9505&p=8815&v=8633)에서 크레온 플러스 API를 신청해야합니다.

## DataBase Configuration
### - conf.ini 작성
```python
[db]
name=<your_db_name>
host=<your_db_url>
port=<your_db_port>
```
---
## Quick Start
**모든 주식의 1분봉 받기**
```
python creon_datareader_mongo.py --tick_unit 1min
```
---
# Reference
1. 대신증권(Creon) PLUS API를 이용한 주가 데이터 수집 프로그램 https://github.com/gyusu/Creon-Datareader
    
