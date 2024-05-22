import pandas as pd
import os
import shutil
from sqlalchemy import create_engine

# pd.set_option("display.max_rows", 1000)
pd.set_option('display.max_rows', 500)

disk_engine = create_engine("sqlite:///../players.db")

CARDLIST_PATH = os.path.expanduser(
    "~/Application Support/Out of the Park Developments/OOTP Baseball 25/online_data/pt_card_list.csv"
)

pd.set_option("display.max_rows", 500)


def get_and_clean():
    CARDLIST_PATH = os.path.expanduser(
        "~/Application Support/Out of the Park Developments/OOTP Baseball 25/online_data/pt_card_list.csv"
    )
    shutil.copyfile(CARDLIST_PATH, "./card_list.csv")

    df = pd.read_csv("./card_list.csv", index_col=False)

    column_rename_dict = {
        "//Card Title": "card_title",
        "Card ID": "card_id",
        "Card Value": "card_value",
        "Card Type": "card_type",
        "Card Sub Type": "card_sub_type",
        "Year": "year",
        "Peak": "peak",
        "Team": "team",
        "LastName": "last_name",
        "FirstName": "first_name",
        "NickName": "nickname",
        "UniformNumber": "uniform_number",
        "DayOB": "day_of_birth",
        "MonthOB": "month_of_birth",
        "YearOB": "year_of_birth",
        "Bats": "bats",
        "Throws": "throws",
        "Position": "position",
        "Pitcher Role": "pitcher_role",
        "Contact": "contact",
        "Gap": "gap",
        "Power": "power",
        "Eye": "eye",
        "Avoid Ks": "avoid_ks",
        "BABIP": "babip",
        "Contact vL": "contact_vs_left",
        "Gap vL": "gap_vs_left",
        "Power vL": "power_vs_left",
        "Eye vL": "eye_vs_left",
        "Avoid K vL": "avoid_k_vs_left",
        "BABIP vL": "babip_vs_left",
        "Contact vR": "contact_vs_right",
        "Gap vR": "gap_vs_right",
        "Power vR": "power_vs_right",
        "Eye vR": "eye_vs_right",
        "Avoid K vR": "avoid_k_vs_right",
        "BABIP vR": "babip_vs_right",
        "GB Hitter Type": "gb_hitter_type",
        "FB Hitter Type": "fb_hitter_type",
        "BattedBallType": "batted_ball_type",
        "Speed": "speed",
        "Steal Rate": "steal_rate",
        "Stealing": "stealing",
        "Baserunning": "baserunning",
        "Sac bunt": "sac_bunt",
        "Bunt for hit": "bunt_for_hit",
        "Stuff": "stuff",
        "Movement": "movement",
        "Control": "control",
        "pHR": "phr",
        "pBABIP": "pbabip",
        "Stuff vL": "stuff_vs_left",
        "Movement vL": "movement_vs_left",
        "Control vL": "control_vs_left",
        "pHR vL": "phr_vs_left",
        "pBABIP vL": "pbabip_vs_left",
        "Stuff vR": "stuff_vs_right",
        "Movement vR": "movement_vs_right",
        "Control vR": "control_vs_right",
        "pHR vR": "phr_vs_right",
        "pBABIP vR": "pbabip_vs_right",
        "Fastball": "fastball",
        "Slider": "slider",
        "Curveball": "curveball",
        "Changeup": "changeup",
        "Cutter": "cutter",
        "Sinker": "sinker",
        "Splitter": "splitter",
        "Forkball": "forkball",
        "Screwball": "screwball",
        "Circlechange": "circlechange",
        "Knucklecurve": "knucklecurve",
        "Knuckleball": "knuckleball",
        "Stamina": "stamina",
        "Hold": "hold",
        "GB": "ground_ball",
        "Velocity": "velocity",
        "Arm Slot": "arm_slot",
        "Height": "height",
        "Infield Range": "infield_range",
        "Infield Error": "infield_error",
        "Infield Arm": "infield_arm",
        "DP": "double_play",
        "CatcherAbil": "catcher_ability",
        "CatcherFrame": "catcher_frame",
        "Catcher Arm": "catcher_arm",
        "OF Range": "outfield_range",
        "OF Error": "outfield_error",
        "OF Arm": "outfield_arm",
        "Pos Rating P": "pos_rating_pitcher",
        "Pos Rating C": "pos_rating_catcher",
        "Pos Rating 1B": "pos_rating_first_base",
        "Pos Rating 2B": "pos_rating_second_base",
        "Pos Rating 3B": "pos_rating_third_base",
        "Pos Rating SS": "pos_rating_shortstop",
        "Pos Rating LF": "pos_rating_left_field",
        "Pos Rating CF": "pos_rating_center_field",
        "Pos Rating RF": "pos_rating_right_field",
        "LearnC": "learn_catcher",
        "Learn1B": "learn_first_base",
        "Learn2B": "learn_second_base",
        "Learn3B": "learn_third_base",
        "LearnSS": "learn_shortstop",
        "LearnLF": "learn_left_field",
        "LearnCF": "learn_center_field",
        "LearnRF": "learn_right_field",
        "era": "era",
        "tier": "tier",
        "MissionValue": "mission_value",
        "limit": "limit",
        "owned": "owned",
        "brefid": "bref_id",
        "Buy Order High": "buy_order_high",
        "Sell Order Low": "sell_order_low",
        "Last 10 Price": "last_10_price",
        "Buy Order High(CC)": "buy_order_high_cc",
        "Sell Order Low(CC)": "sell_order_low_cc",
        "Last 10 Price(CC)": "last_10_price_cc",
        "date": "date",
    }

    df.rename(columns=column_rename_dict, inplace=True)

    df.set_index("card_id", inplace=True)

    strip = [
        "uniform_number",
        "day_of_birth",
        "month_of_birth",
        "year_of_birth",
        "mission_value",
        "limit",
        "owned",
        "buy_order_high",
        "sell_order_low",
        "last_10_price",
        "buy_order_high_cc",
        "sell_order_low_cc",
        "last_10_price_cc",
        "date",
    ]

    df.drop(strip, inplace=True, axis=1)

    card_type_map = {
        1: "Live",
        2: "Negro League Star",
        3: "Rookie Sensation",
        4: "Legend",
        5: "All Star",
        6: "Future Legend",
        7: "Snapshot",
        8: "Unsung Heroes",
        9: "Hardware Heroes",
        10: "Special Edition"

    }

    gb_map = {
        0: "extreme_groundball",
        1: "groundball",
        2: "neutral",
        3: "flyball",
        4: "extreme_flyball",
    }

    hand_map = {1: "R", 2: "L", 3: "S"}

    pitcher_role_map = {11: "SP", 12: "RP", 13: "CL"}

    arm_slot_map = {1: "submarine", 2: "sidearm", 3: "normal", 4: "over_top"}

    df["ground_ball_type"] = df["ground_ball"].map(gb_map)

    df["bats"] = df["bats"].map(hand_map)
    df["throws"] = df["throws"].map(hand_map)

    df["pitcher_role"] = df["pitcher_role"].map(pitcher_role_map)
    df["arm_slot"] = df["arm_slot"].map(arm_slot_map)
    
    df['card_type'] = df['card_type'].map(card_type_map)

    # df.to_csv("./card_list.csv")
    # df.to_json("./card_list.json")
    df.to_sql("cards", disk_engine, if_exists="replace")
    # print(df[df['card_type'] == 10].head(500))

    # print(df.groupby("card_type").agg({
    #     "card_title": "first"
    # }))


get_and_clean()
