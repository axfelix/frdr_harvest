class TimeFormatter:
    def __init__(self):
        self.nothing = None

    def humanize(self, amount):
        INTERVALS = [1, 60, 3600, 86400, 604800, 2629800, 31557600]
        NAMES = [('second', 'seconds'), ('minute', 'minutes'), ('hour', 'hours'),
                 ('day', 'days'), ('week', 'weeks'), ('month', 'months'), ('year', 'years')]
        result = ""
        amount = int(amount)

        for i in range(len(NAMES) - 1, -1, -1):
            a = amount // INTERVALS[i]
            if a > 0:
                result = result + str(a) + " " + str(NAMES[i][1 % a]) + " "
                amount -= a * INTERVALS[i]

        result = str.strip(result)
        if result == "":
            result = "0 seconds"
        return result
