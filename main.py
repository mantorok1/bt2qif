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

    write_to_qif_file(lines, repo.filename)


def write_to_qif_file(lines, filename):
    with open(f"./qif/{filename}", 'w') as file:
        for line in lines:
            file.write(f"{line}\n")  

convert()
