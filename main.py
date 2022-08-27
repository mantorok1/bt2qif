import os
from bt_repo import BtRepo
from qif_formatter import QifFormatter

def convert():
    repo = BtRepo()
    formatter = QifFormatter()

    securities = repo.get_funds()
    prices = repo.get_fund_prices()

    investments = []
    investments.extend(repo.get_cash_incomes())
    investments.extend(repo.get_cash_expenses())
    investments.extend(repo.get_cash_deposits())
    investments.extend(repo.get_cash_withdrawal())
    investments.extend(repo.get_fund_incomes())
    investments.extend(repo.get_fund_buys())
    investments.extend(repo.get_fund_sells())

    lines = []
    lines.extend(formatter.format_securities(securities))
    lines.extend(formatter.format_prices(prices))
    lines.extend(formatter.format_investments(investments))

    save_qif_file(lines, repo.filename)

    repo.save_transactions()


def save_qif_file(lines, filename):
    filepath = os.path.join("qif", filename)

    check_filepath = filepath
    i = 0
    while os.path.exists(check_filepath):
        i = i + 1
        check_filepath = "{0}_{2}{1}".format(*os.path.splitext(filepath) + (i,))

    filepath = check_filepath

    with open(filepath, 'w') as file:
        for line in lines:
            file.write(f"{line}\n")  

convert()
