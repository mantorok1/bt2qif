import pandas as pd
import json
import sys
from datetime import datetime

class BtRepo:
    def __init__(self):
        self.transactions = pd.read_csv("./csv/transactionHistoryCsvReportV3.csv")
        self.portfolio_values = pd.read_csv("./csv/portfolioValuationCsvReport.csv")

        try:
            with open("./config/config.json") as file:
                config = json.load(file)
        except FileNotFoundError:
            config = {
                "default": "bt",
                "bt": dict()
            }

        if not config.get("default"):
            config["default"] = list(config.keys())[0]

        account = sys.argv[1] if len(sys.argv) > 1 else config.get("default")
        if account is None:
            raise Exception("No default account defined")
        if config.get(account) is None:
            raise Exception(f"Account {account} not defined")
        self.category_map = config[account]

        self.filename = f"{account}_{datetime.today().strftime('%Y-%m-%d')}.qif"

    def get_funds(self):
        records = []

        trans_funds = self.transactions[self.transactions["Investment type"] == "Managed fund"]
        securities = trans_funds[pd.notna(trans_funds["Security"])]["Security"].unique()
        for fund_code, fund_name in [(fund[0:9], fund[12:]) for fund in securities]:
            records.append({
                "code": fund_code,
                "name": fund_name,
                "type": "Fund or Trust"
            })

        if self.portfolio_values is None:
            return records

        fund_codes = [fund["code"] for fund in records]
        self.funds = {fund["code"]: fund["name"] for fund in records}

        port_funds = self.portfolio_values[self.portfolio_values["Investment type"] == "Managed funds"]
        port_funds = port_funds[pd.notna(port_funds["Asset code"])]
        for index, row in port_funds.iterrows():
            if row["Asset code"] not in fund_codes:
                records.append({
                    "code": row["Asset code"],
                    "name": row["Asset name"],
                    "type": "Fund or Trust"
                })                

        return records

    def get_fund_prices(self):
        records = []
        
        if self.portfolio_values is None:
            return records

        port_funds = self.portfolio_values[self.portfolio_values["Investment type"] == "Managed funds"]
        port_funds = port_funds[pd.notna(port_funds["Asset code"])]
        for index, row in port_funds.iterrows():
            fund_code = row["Asset code"]
            fund_price = row["Last price $"]
            price_date = datetime.strptime(row["Last price date"], "%d-%b-%y").strftime("%d/%m/%Y")
            records.append({
                "code": fund_code,
                "price": fund_price,
                "price_date": price_date
            })

        return records

    def get_cash_incomes(self):
        records = []
        if not self.category_map.get("Interest"):
            return records

        incomes = self.transactions.query("`Investment type` in ['Cash Management Account', 'Cash'] and `Transaction type` == 'Income'")
        incomes = incomes[pd.isna(incomes["Security"])]

        for index, row in incomes[::-1].iterrows():
            if "Interest" != row["Description"][0:8]:
                continue

            records.append({
                "trade_date": row["Trade date"],
                "action": "Cash",
                "memo": row["Description"],
                "amount": row["Net amount $"],
                "category": self.category_map.get("Interest")
            })

        return records

    def get_cash_expenses(self):
        expenses = self.transactions.query("`Investment type` in ['Cash Management Account', 'Cash'] and `Transaction type` == 'Expense'")
        expenses = expenses[pd.isna(expenses["Security"])]

        records = []
        for index, row in expenses[::-1].iterrows():
            category = "OtherFee"
            if row["Description"].startswith("Advice fee"):
                category = "AdviceFee"
            elif row["Description"].startswith("Administration fee"):
                category = "AdminFee"
            elif row["Description"].startswith("Ongoing Adviser Fee"):
                category = "AdviceFee"
            elif row["Description"].startswith("Account Keeping Fee"):
                category = "AdminFee"

            if not self.category_map.get(category):
                continue

            records.append({
                "trade_date": row["Trade date"],
                "action": "Cash",
                "memo": row["Description"],
                "amount": row["Net amount $"],
                "category": self.category_map[category]
            })

        return records

    def get_cash_deposits(self):
        records = []
        if not self.category_map.get("Deposit"):
            return records

        deposits = self.transactions.query("`Investment type` in ['Cash Management Account', 'Cash'] and `Transaction type` == 'Deposit'")

        for index, row in deposits[::-1].iterrows():
            records.append({
                "trade_date": row["Trade date"],
                "action": "Cash",
                "memo": row["Description"],
                "amount": row["Net amount $"],
                "category": self.category_map["Deposit"]
            })

        return records

    def get_cash_withdrawal(self):
        records = []
        if not self.category_map.get("Withdrawal"):
            return records

        withdrawals = self.transactions.query("`Investment type` in ['Cash Management Account', 'Cash'] and `Transaction type` == 'Payment'")

        for index, row in withdrawals[::-1].iterrows():
            records.append({
                "trade_date": row["Trade date"],
                "action": "Cash",
                "memo": row["Description"],
                "amount": row["Net amount $"],
                "category": self.category_map["Withdrawal"]
            })

        return records

    def get_fund_buys(self):
        fund_buys = self.transactions.query("`Investment type` == 'Managed fund' and `Transaction type` == 'Buy'")
        fund_buys = fund_buys[pd.notna(fund_buys["Security"])]

        records = []
        for index, row in fund_buys[::-1].iterrows():
            commission = self._get_transaction_fee(row['Description'])
            amount = self._get_transaction_amount(index, row, "Buy")
            amount = amount - commission

            records.append({
                "trade_date": row["Trade date"],
                "action": "Buy",
                "memo": row["Description"],
                "security": row['Security'][12:],
                "quantity": row['Units'],
                "amount": amount,
                "commission": commission
            })

        return records

    def get_fund_sells(self):
        fund_sells = self.transactions.query("`Investment type` == 'Managed fund' and `Transaction type` == 'Sell'")
        fund_sells = fund_sells[pd.notna(fund_sells["Security"])]

        records = []
        for index, row in fund_sells[::-1].iterrows():
            commission = self._get_transaction_fee(row['Description'])
            amount = self._get_transaction_amount(index, row, "Sell")
            amount = amount - commission

            records.append({
                "trade_date": row["Trade date"],
                "action": "Sell",
                "memo": row["Description"],
                "security": row['Security'][12:],
                "quantity": abs(row['Units']),
                "amount": amount,
                "commission": commission
            })

        return records

    def _get_transaction_amount(self, min_index, row, trans_type):
        cash_trans = self.transactions.query("`Investment type` in ['Cash Management Account', 'Cash'] and `Transaction type` == @trans_type")

        fund_code = row["Security"][0:9]
        fund_name = row["Security"][12:57]
        search_fund = f"{fund_name} ({fund_code})"

        for index, cash_tran in cash_trans[cash_trans.index > min_index].iterrows():
            if search_fund in cash_tran["Description"]:
                return abs(float(cash_tran["Net amount $"]))

        for index, cash_tran in cash_trans.iterrows():
            if row["Description"] == cash_tran["Description"]:
                 return abs(float(cash_tran["Net amount $"]))           

        return 0

    def _get_transaction_fee(self, description):
        index = description.find("Transaction fee $")
        if index < 0:
            return 0
        return abs(float(description[index+17:-1]))


    def get_fund_incomes(self):
        incomes = self.transactions.query("`Investment type` == 'Cash Management Account' and `Transaction type` == 'Income'")
        incomes = incomes[pd.notna(incomes["Security"])]

        records = []
        for index, row in incomes[::-1].iterrows():
            records.append({
                "trade_date": row["Trade date"],
                "action": "CGLong",
                "memo": row["Description"],
                "security": row['Security'][12:],
                "amount": row["Net amount $"]
            })

        incomes = self.transactions.query("`Investment type` == 'Cash' and `Transaction type` == 'Income'")

        for index, row in incomes[::-1].iterrows():
            if not row["Description"].startswith("Distribution"):
                continue

            fund_code = row["Description"][-9:]
            fund_name = self.funds[fund_code]

            records.append({
                "trade_date": row["Trade date"],
                "action": "CGLong",
                "memo": row["Description"],
                "security": fund_name,
                "amount": row["Net amount $"]
            })        

        return records
