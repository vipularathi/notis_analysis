from datetime import datetime, timedelta, timezone

class Utility:

    @staticmethod
    def get_date_from_jiffy(dt_val):
        """
        This method is used to convert the Jiffy format date to a readable format.
        :param dt_val: long
        :return: long
        """
        return (dt_val // 65536) + 315513000

    @staticmethod
    def get_date_from_non_jiffy(dt_val):
        """
        This method is used to convert the 1980 format date time to a readable format.
        :param dt_val: long
        :return: long
        """
        return dt_val + 315513000

    @staticmethod
    def get_date_from_non_jiffy1(dt_val):
        """
        This method is used to convert the 1970 format date time to a readable format.
        :param dt_val: long
        :return: long
        """
        return dt_val

    @staticmethod
    def get_date_from_jiffya(dt_val):
        """
        Converts the Jiffy format date to a readable format.
        :param dt_val: long
        :return: long (epoch time in seconds)
        """
        # Jiffy is 1/65536 of a second since Jan 1, 1980
        base_date = datetime(1980, 1, 1, tzinfo=timezone.utc)
        return int((base_date.timestamp() + (dt_val / 65536)))

    @staticmethod
    def get_date_from_non_jiffya(dt_val):
        """
        Converts the 1980 format date time to a readable format.
        :param dt_val: long
        :return: long (epoch time in seconds)
        """
        # Assuming dt_val is seconds since Jan 1, 1980
        base_date = datetime(1980, 1, 1, tzinfo=timezone.utc)
        return int(base_date.timestamp() + dt_val)

    @staticmethod
    def get_date_from_non_jiffy1a(dt_val):
        """
        Converts the 1970 format date time to a readable format.
        :param dt_val: long
        :return: long (epoch time in seconds)
        """
        # dt_val is already in seconds since Unix epoch (1970)
        return dt_val


if __name__ == "__main__":
    # Jiffy format
    date_time = Utility.get_date_from_jiffy(92926374141189)
    new_date = datetime.fromtimestamp(date_time, timezone.utc)
    formatted_date = new_date.astimezone(timezone(timedelta(hours=5, minutes=30))).strftime("%d/%m/%Y %I:%M:%S %p")
    print(f"date from jiffy format:=========>{formatted_date}")

    # 1980 format
    date_time1 = Utility.get_date_from_non_jiffy(1417943941)
    new_date1 = datetime.fromtimestamp(date_time1, timezone.utc)
    formatted_date1 = new_date1.astimezone(timezone(timedelta(hours=5, minutes=30))).strftime("%d/%m/%Y %I:%M:%S %p")
    print(f"date from 1980 format:=========>{formatted_date1}")

    # 1970 format
    date_time2 = Utility.get_date_from_non_jiffy1(1417943941)
    new_date2 = datetime.fromtimestamp(date_time2, timezone.utc)
    formatted_date2 = new_date2.astimezone(timezone(timedelta(hours=5, minutes=30))).strftime("%d/%m/%Y %I:%M:%S %p")
    print(f"date from 1970 format:=========>{formatted_date2}")

    # Jiffy format
    print('New')
    date_time = Utility.get_date_from_jiffya(92926374141189)
    new_date = datetime.fromtimestamp(date_time, timezone.utc)
    formatted_date = new_date.astimezone(timezone(timedelta(hours=5, minutes=30))).strftime("%d/%m/%Y %I:%M:%S %p")
    print(f"date from jiffy format:=========>{formatted_date}")

    # 1980 format
    date_time1 = Utility.get_date_from_non_jiffya(1417943941)
    new_date1 = datetime.fromtimestamp(date_time1, timezone.utc)
    formatted_date1 = new_date1.astimezone(timezone(timedelta(hours=5, minutes=30))).strftime("%d/%m/%Y %I:%M:%S %p")
    print(f"date from 1980 format:=========>{formatted_date1}")

    # 1970 format
    date_time2 = Utility.get_date_from_non_jiffy1a(1417943941)
    new_date2 = datetime.fromtimestamp(date_time2, timezone.utc)
    formatted_date2 = new_date2.astimezone(timezone(timedelta(hours=5, minutes=30))).strftime("%d/%m/%Y %I:%M:%S %p")
    print(f"date from 1970 format:=========>{formatted_date2}")