RECORD_END = "^"


class QifFormatter:
    def format_securities(self, records):
        lines = []
        for record in records:
            lines.extend([
                "!Type:Security",
                f"N{record['name']}",
                f"S{record['code']}",
                f"T{record['type']}",
                RECORD_END
            ])

        return lines

    def format_prices(self, records):
        lines = []
        for record in records:
            lines.extend([
                "!Type:Prices",
                f"\"{record['code']}\",{record['price']},\"{record['price_date']}\"",
                RECORD_END
            ])

        return lines

    def format_investments(self, records):
        if len(records) == 0:
            return []

        lines = ["!Type:Invst"]
        for record in records:
            lines.extend([
                f"D{record['date']}",
                f"N{record['action']}",
                f"M{record['memo']}",
                f"U{record['amount']}",
                f"T{record['amount']}"
            ])
            if "security" in record:
                lines.append(f"Y{record['security']}")
            if "quantity" in record:
                lines.append(f"Q{record['quantity']}")
            if "commission" in record:
                lines.append(f"O{record['commission']}")
            if "category" in record:
                lines.append(f"L{record['category']}")
            lines.append(RECORD_END)

        return lines
