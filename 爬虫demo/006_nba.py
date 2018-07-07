#!/usr/bin/python3
# -*- coding: utf-8 -*-


# ============================================================
# 内容描述： 抓取  虎扑网  NBA2017-2018赛季的数据
# ============================================================

from urllib.request import urlopen
from urllib.request import Request
from bs4 import BeautifulSoup
import time
import csv


# 模拟了使用浏览器在访问
head = {"User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.75 Safari/537.36",
                "Cookie":"_cnzz_CV30020080=buzi_cookie%7C49a8de20.3afa.4c0e.a772.f2f7174eaea2%7C-1; _dacevid3=49a8de20.3afa.4c0e.a772.f2f7174eaea2; __gads=ID=da6d3191f88371a5:T=1521608571:S=ALNI_MaXI59MepaMyFsxgTz1gP5SrqF29Q; ADHOC_MEMBERSHIP_CLIENT_ID1.0=bc4fb93f-9ba6-be6e-0e89-8d4f6fc4d8c3; _cnzz_CV30020080=buzi_cookie%7C49a8de20.3afa.4c0e.a772.f2f7174eaea2%7C-1; __dacevst=e1b03078.d2274d51|1522416769306; Hm_lvt_2eb807cf60e3295559f967a907218f33=1521609559,1521609559,1521609559,1522407680; Hm_lpvt_2eb807cf60e3295559f967a907218f33=1522414969"}

# 打开两个文件对象，用来写入比赛结果
nba_game_file = open("c:/nba_game.csv", mode="w", encoding="utf-8")
nba_game_detail_file = open("c:/nba_game_detail.csv", mode="w", encoding="utf-8")
nba_team_file = open("c:/nba_team.csv", mode="w", encoding="utf-8")
csv_writer_game = csv.writer(nba_game_file)
csv_writer_game_detail = csv.writer(nba_game_detail_file)
nba_team_writer = csv.writer(nba_team_file)

# 这是所有球队赛程的入口网址
url = "https://nba.hupu.com/schedule"


# 获取所有NBA球队的比赛列表的入口URL，和球队名称
def get_all_team_url(url):
    request = Request(url, headers=head)
    team_response = urlopen(request)
    team_bs = BeautifulSoup(team_response, "html.parser")
    team_list = team_bs.select("span.team_name a")
    team_url = [t.get("href") for t in team_list]
    team_names = [t.text for t in team_list]
    return team_url,team_names


# 获取一个球队的所有比赛的列表
def get_game_data_detail(game_url, gameid):
    game_request = Request(game_url, headers=head)
    game_response = urlopen(game_request)
    game_bs = BeautifulSoup(game_response, "html.parser")

    section_away_tds = game_bs.select("tr.away_score > td")
    section_home_tds = game_bs.select("tr.home_score > td")
    kedui_name = section_away_tds.pop(0).text
    zhudui_name = section_home_tds.pop(0).text
    section_away_tds.pop(-1)
    section_home_tds.pop(-1)
    section_away_score = "-".join([section_td.text.strip() for section_td in section_away_tds])
    section_home_score = "-".join([section_td.text.strip() for section_td in section_home_tds])
    game_time_p = game_bs.select("p.consumTime")
    game_time = game_time_p[0].text.split("：")[1]
    peoplenum_p = game_bs.select("p.peopleNum")
    peoplenum = peoplenum_p[0].text.split("：")[1].strip("人")

    # 获取客队的比赛成绩
    trs = game_bs.select("table#J_away_content > tbody > tr")
    # 获取主队成绩
    trs_home = game_bs.select("table#J_home_content > tbody > tr")
    trss = [trs, trs_home]
    team_ids = [str(team_dict.get(kedui_name)), str(team_dict.get(zhudui_name))]
    for ii in range(len(trss)):
        mytrs = trss[ii]
        team_id = team_ids[ii]
        if len(mytrs) >= 5:
            is_start_role = 1
            for i in range(len(mytrs)):
                player_or_role = mytrs[i].select("td")[0].text
                if player_or_role == "首发":
                    is_start_role = 1
                elif player_or_role == "替补":
                    is_start_role = 0
                elif player_or_role == "统计":
                    pass
                elif player_or_role == "命中率":
                    pass
                else:
                    # 比赛ID， 客场队伍0-主场队伍1，首发1-不是0，
                    one_player_one_game_detail = [str(gameid), team_id, str(ii), str(is_start_role)] + list(mytrs[i].stripped_strings)
                    ## 此处打印的是一个球队的 所有比赛结果情况
                    print("\t".join(one_player_one_game_detail))
                    csv_writer_game_detail.writerow(one_player_one_game_detail)

    return [section_away_score, section_home_score, game_time, peoplenum]


# 用来存储所有的比赛信息。一场比赛，一个key-valye
# key是  比赛队伍+比赛时间  ， 比如：黄蜂-公牛-2018-04-04 08:00:00
# value是   一个全局唯一的gameid
game_result_dict = {}


# 获取每一支球队的所有比赛的详细信息
def get_team_all_bisai_data(url_team, team_name, game_result_dict_value, team_dict):

    team_request1 = Request(url_team, headers=head)
    team_response1 = urlopen(team_request1)
    game_bs = BeautifulSoup(team_response1, "html.parser")

    kedui = []
    zhudui = []
    kedui_score = []
    zhudui_score = []
    result = []
    dt = []
    data_url = []
    is_start = []

    # 返回有效的要爬取比赛详细结果的URL
    scawl_game_url = []

    game_list = game_bs.select("tr.left")
    for game in game_list:
        if len(game.get("class")) == 1:
            five_td = game.select("td")

            # 两支队伍
            vs = list(five_td[0].stripped_strings)
            kedui.append(vs[0])
            zhudui.append(vs[2])

            # 比分
            score = list(five_td[1].stripped_strings)[0].replace("\xa0", "").split("-")
            if bool(score[0]):
                kedui_score.append(score[0])
                zhudui_score.append(score[1])
            else:
                kedui_score.append("0")
                zhudui_score.append("0")

            # 比赛结果
            result.append(five_td[2].text.strip())

            # 比赛时间
            dt.append(five_td[3].text.strip())

            # 详细数据
            # 是否开打
            data_a = five_td[4].select("a")[0]
            data_url.append(data_a.get("href"))
            is_start.append(data_a.text)

    for i in range(len(kedui)):

        dict_key = kedui[i] + "-" + zhudui[i] + "-" + dt[i]
        if dict_key in game_result_dict:
            pass
        else:
            # gameid变量是 每一场比赛一个唯一的ID
            gameid = game_result_dict_value
            game_result_dict_value += 1
            game_result_dict[dict_key] = gameid

            scawl_game_url.append(data_url[i])
            # 再去爬取每一场比赛的详细统计信息
            if is_start[i] == "数据统计":
                away_home_score_list = get_game_data_detail(data_url[i], gameid)

                away_score = away_home_score_list[0]
                home_score = away_home_score_list[1]
                game_time = away_home_score_list[2]
                people_number = away_home_score_list[3]

                one_game = [str(gameid), str(team_dict.get(kedui[i])), str(team_dict.get(zhudui[i])), kedui[i], zhudui[i], kedui_score[i], away_score, zhudui_score[i], home_score, result[i], dt[i], game_time, people_number, data_url[i]]
                print("\t".join(one_game))
                # 真正输出每一场比赛结果数据的语句
                csv_writer_game.writerow(one_game)

    return scawl_game_url


team_dict = {"休斯顿火箭":1, "新奥尔良鹈鹕":2, "圣安东尼奥马刺":3, "达拉斯独行侠":4, "孟菲斯灰熊":5,
             "金州勇士": 6,"洛杉矶快船":7,"洛杉矶湖人":8,"萨克拉门托国王":9,"菲尼克斯太阳":10,
             "犹他爵士": 11,"波特兰开拓者":12,"俄克拉荷马城雷霆":13,"明尼苏达森林狼":14,"丹佛掘金":15,
             "多伦多猛龙": 16,"波士顿凯尔特人":17,"费城76人":18,"纽约尼克斯":19,"布鲁克林篮网":20,
             "迈阿密热火": 21,"华盛顿奇才":22,"夏洛特黄蜂":23,"奥兰多魔术":24,"亚特兰大老鹰":25,
             "克利夫兰骑士": 26,"印第安纳步行者":27,"密尔沃基雄鹿":28,"底特律活塞":29,"芝加哥公牛":30,
             "火箭": 1, "鹈鹕": 2, "马刺": 3, "独行侠": 4, "灰熊": 5,
             "勇士": 6, "快船": 7, "湖人": 8, "国王": 9, "太阳": 10,
             "爵士": 11, "开拓者": 12, "雷霆": 13, "森林狼": 14, "掘金": 15,
             "猛龙": 16, "凯尔特人": 17, "76人": 18, "尼克斯": 19, "篮网": 20,
             "热火": 21, "奇才": 22, "黄蜂": 23, "魔术": 24, "老鹰": 25,
             "骑士": 26, "步行者": 27, "雄鹿": 28, "活塞": 29, "公牛": 30}
team_district = ["西南赛区", "太平洋赛区", "西北赛区", "大西洋赛区", "东南赛区", "中部赛区"]
west_and_east = ["东部", "西部"]

##  存储所有球队的比赛结果
total_game_data_url = []
##  获取所有球队的入口URL 和  球队名称
team_names = get_all_team_url(url)
teams = team_names[0]
names = team_names[1]

for i in range(len(team_names[0])):
    district_index = i//5 + 1
    west_and_east_index = i//15
    one_team = [str(team_dict.get(names[i])), str(west_and_east_index), team_district[district_index-1], str(district_index), teams[i], names[i]]
    # 20,1,4,https://nba.hupu.com/players/nets,布鲁克林篮网
    print(",".join(one_team))
    nba_team_writer.writerow(one_team)
nba_team_file.close()


game_result_dict_value = 1
for i in range(len(teams)):
    # print(names[i],teams[i])
    # 获取所有球队的所有比赛结果 及 详细 信息
    data_url = get_team_all_bisai_data(teams[i], names[i], game_result_dict_value, team_dict)
    total_game_data_url += data_url
    game_result_dict_value += len(data_url)
    
nba_game_file.close()
nba_game_detail_file.close()