import pandas as pd
import os
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

        self.account = sys.argv[1] if len(sys.argv) > 1 else config.get("default")
        if self.account is None:
            raise Exception("No default account defined")
        if config.get(self.account) is None:
            raise Exception(f"Account {self.account} not defined")
        self.category_map = config[self.account]

        self.filename = f"{self.account}_{datetime.today().strftime('%Y-%m-%d')}.qif"

        # Get previous transactions
        self.processed_transactions_filename = os.path.join("config", f"processed_transactions_{self.account}.json")
        try:
            with open(self.processed_transactions_filename, "r") as f:
                self.processed_transactions = json.load(f)
        except FileNotFoundError:
            self.processed_transactions = dict()


    def get_funds(self):
        # Get unique securities from Portfolio Values
        pv_securities = self.portfolio_values[self.portfolio_values["Investment type"] == "Managed funds"]
        pv_securities = pv_securities[["Asset code", "Asset name"]].dropna()

        # Get unique securities from Transactions
        t_securities = self.transactions[self.transactions["Investment type"] == "Managed fund"]
        t_securities = pd.DataFrame(t_securities["Security"].drop_duplicates())
        t_securities = t_securities["Security"].str.split(" - ", n=1, expand=True)
        t_securities.rename(columns={0: "Asset code", 1: "Asset name"}, inplace=True)

        # Get all unique securities
        securities = pd.concat([pv_securities, t_securities]).drop_duplicates()

        records = []
        for index, row in securities.iterrows():
            records.append({
                "code": row["Asset code"],
                "name": row["Asset name"],
                "type": "Fund or Trust"
            })           

        print(f" Processed {len(records)} securities")

        return records

    def get_fund_prices(self):
        # Get unique securities from Portfolio Values
        securities = self.portfolio_values[self.portfolio_values["Investment type"] == "Managed funds"]
        securities = securities[["Asset code", "Last price date", "Last price $"]].dropna()

        records = []
        for index, row in securities.iterrows():
            fund_code = row["Asset code"]
            fund_price = row["Last price $"]
            price_date = datetime.strptime(row["Last price date"], "%d-%b-%y").strftime("%d/%m/%Y")
            records.append({
                "code": fund_code,
                "price": fund_price,
                "price_date": price_date
            })

        print(f" Processed {len(records)} prices")

        return records

    def get_cash_incomes(self):
        incomes = self.transactions.query("`Investment type` in ['Cash Management Account', 'Cash'] and `Transaction type` == 'Income'")
        incomes = incomes[pd.isna(incomes["Security"])]

        incomes = incomes[["Settlement date", "Description", "Net amount $"]]

        records = []
        for index, row in incomes[::-1].iterrows():
            category = "OtherIncome"
            if row["Description"].startswith("Interest"):
                category = "Interest"

            if not self.category_map.get(category):
                continue

            record = {
                "date": row["Settlement date"],
                "action": "Cash",
                "memo": row["Description"],
                "amount": row["Net amount $"],
                "category": self.category_map.get(category),
            }

            if (record_key := self.get_key(record)) not in self.processed_transactions:
                records.append(record)
                self.processed_transactions[record_key] = record

        print(f" Processed {len(records)} Cash income transactions")

        return records

    def get_cash_expenses(self):
        expenses = self.transactions.query("`Investment type` in ['Cash Management Account', 'Cash'] and `Transaction type` == 'Expense'")
        expenses = expenses[pd.isna(expenses["Security"])]

        expenses = expenses[["Settlement date", "Description", "Net amount $"]]

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

            record = {
                "date": row["Settlement date"],
                "action": "Cash",
                "memo": row["Description"],
                "amount": row["Net amount $"],
                "category": self.category_map[category],
            }

            if (record_key := self.get_key(record)) not in self.processed_transactions:
                records.append(record)
                self.processed_transactions[record_key] = record

        print(f" Processed {len(records)} Cash expense transactions")

        return records

    def get_cash_deposits(self):
        records = []
        if not self.category_map.get("Deposit"):
            return records

        deposits = self.transactions.query("`Investment type` in ['Cash Management Account', 'Cash'] and `Transaction type` == 'Deposit'")

        deposits = deposits[["Settlement date", "Description", "Net amount $"]]

        for index, row in deposits[::-1].iterrows():
            record = {
                "date": row["Settlement date"],
                "action": "Cash",
                "memo": row["Description"],
                "amount": row["Net amount $"],
                "category": self.category_map["Deposit"],
            }

            if (record_key := self.get_key(record)) not in self.processed_transactions:
                records.append(record)
                self.processed_transactions[record_key] = record

        print(f" Processed {len(records)} Cash deposit transactions")

        return records

    def get_cash_withdrawal(self):
        records = []
        if not self.category_map.get("Withdrawal"):
            return records

        withdrawals = self.transactions.query("`Investment type` in ['Cash Management Account', 'Cash'] and `Transaction type` == 'Payment'")

        withdrawals = withdrawals[["Settlement date", "Description", "Net amount $"]]

        for index, row in withdrawals[::-1].iterrows():
            record = {
                "date": row["Settlement date"],
                "action": "Cash",
                "memo": row["Description"],
                "amount": row["Net amount $"],
                "category": self.category_map["Withdrawal"],
            }

            if (record_key := self.get_key(record)) not in self.processed_transactions:
                records.append(record)
                self.processed_transactions[record_key] = record

        print(f" Processed {len(records)} Cash withdrawal transactions")

        return records

    def get_fund_buys(self):
        fund_buys = self.transactions.query("`Investment type` == 'Managed fund' and `Transaction type` == 'Buy'")
        fund_buys = fund_buys[pd.notna(fund_buys["Security"])]
        if len(fund_buys) == 0:
            print(" Processed 0 Buy transactions")
            return []

        fund_buys[["Asset code", "Asset name"]] = fund_buys["Security"].str.split(" - ", n=1, expand=True)
        fund_buys["Transaction fee"] = fund_buys["Description"].apply(self._get_transaction_fee)
        fund_buys["Net amount $"] = fund_buys.apply(lambda x: self._get_buy_transaction_amount(x["Asset code"], x["Trade date"], x["Description"]), axis=1)

        fund_buys = fund_buys[["Settlement date", "Description", "Asset name", "Units", "Net amount $", "Transaction fee"]]

        records = []
        for index, row in fund_buys[::-1].iterrows():
            record = {
                "date": row["Settlement date"],
                "action": "Buy",
                "memo": row["Description"],
                "security": row["Asset name"],
                "quantity": row["Units"],
                "amount": row["Net amount $"] - row["Transaction fee"],
                "commission": row["Transaction fee"],
            }

            if (record_key := self.get_key(record)) not in self.processed_transactions:
                records.append(record)
                self.processed_transactions[record_key] = record

        print(f" Processed {len(records)} Buy transactions")

        return records

    def _get_buy_transaction_amount(self, fund_code, trade_date, description):
        cash_trans = self.transactions.query("`Investment type` in ['Cash Management Account', 'Cash'] and `Transaction type` == 'Buy'")
        cash_trans = cash_trans[cash_trans["Description"].str.contains(f"({fund_code})")]
        cash_trans = cash_trans[cash_trans["Trade date"].apply(lambda x: datetime.strptime(x, "%d/%m/%Y")) <= datetime.strptime(trade_date, "%d/%m/%Y")]

        # This is the best way I found to match the CMA record with the Buy record 
        if description.startswith("Regular Investment Buy"):
            cash_trans = cash_trans[cash_trans["Description"].str.startswith("Regular Investment Buy")]
        else:
            cash_trans = cash_trans[~cash_trans["Description"].str.startswith("Regular Investment Buy")]

        if len(cash_trans.index) == 0:
            return 0

        return abs(cash_trans.iloc[0]["Net amount $"])

    def get_fund_sells(self):
        fund_sells = self.transactions.query("`Investment type` == 'Managed fund' and `Transaction type` == 'Sell'")
        fund_sells = fund_sells[pd.notna(fund_sells["Security"])]
        if len(fund_sells) == 0:
            print(f" Processed 0 Sell transactions")
            return []

        fund_sells[["Asset code", "Asset name"]] = fund_sells["Security"].str.split(" - ", n=1, expand=True)
        fund_sells["Transaction fee"] = fund_sells["Description"].apply(self._get_transaction_fee)
        fund_sells["Net amount $"] = fund_sells.apply(lambda x: self._get_sell_transaction_amount(x["Asset code"], x["Trade date"], x["Description"]), axis=1)

        fund_sells = fund_sells[["Settlement date", "Description", "Asset name", "Units", "Net amount $", "Transaction fee"]]

        records = []
        for index, row in fund_sells[::-1].iterrows():
            record = {
                "date": row["Settlement date"],
                "action": "Sell",
                "memo": row["Description"],
                "security": row["Asset name"],
                "quantity": abs(row["Units"]),
                "amount": row["Net amount $"] - row["Transaction fee"],
                "commission": row["Transaction fee"],
            }

            if (record_key := self.get_key(record)) not in self.processed_transactions:
                records.append(record)
                self.processed_transactions[record_key] = record

        print(f" Processed {len(records)} Sell transactions")

        return records

    def _get_sell_transaction_amount(self, fund_code, trade_date, description):
        cash_trans = self.transactions.query("`Investment type` in ['Cash Management Account', 'Cash'] and `Transaction type` == 'Sell'")
        cash_trans = cash_trans[cash_trans["Description"] == description]
        cash_trans = cash_trans[cash_trans["Trade date"].apply(lambda x: datetime.strptime(x, "%d/%m/%Y")) <= datetime.strptime(trade_date, "%d/%m/%Y")]

        if len(cash_trans.index) == 0:
            return 0

        return abs(cash_trans.iloc[0]["Net amount $"])

    def _get_transaction_fee(self, description):
        index = description.find("Transaction fee $")
        if index < 0:
            return 0
        return abs(float(description[index+17:-1]))


    def get_fund_incomes(self):
        incomes = self.transactions.query("`Investment type` == 'Cash Management Account' and `Transaction type` == 'Income'")
        incomes = incomes[pd.notna(incomes["Security"])]

        if len(incomes) == 0:
            print(f" Processed 0 Security income transactions")
            return []

        incomes[["Asset code", "Asset name"]] = incomes["Security"].str.split(" - ", n=1, expand=True)

        incomes = incomes[["Settlement date", "Description", "Asset name", "Net amount $"]]

        records = []
        for index, row in incomes[::-1].iterrows():
            record = {
                "date": row["Settlement date"],
                "action": "CGLong",
                "memo": row["Description"],
                "security": row["Asset name"],
                "amount": row["Net amount $"],
            }

            if (record_key := self.get_key(record)) not in self.processed_transactions:
                records.append(record)
                self.processed_transactions[record_key] = record

        print(f" Processed {len(records)} Security income transactions")

        return records

    def get_key(self, record):
        return self.sha1(json.dumps(record))

    def sha1(self, value):
        from hashlib import sha1
        return sha1(value.encode()).hexdigest()

    def save_transactions(self):
        with open(self.processed_transactions_filename, "w") as f:
            json.dump(self.processed_transactions, f)
