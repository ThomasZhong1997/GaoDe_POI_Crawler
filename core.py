import requests
import Distance as dt
import math
import json
from multiprocessing import Process
from multiprocessing import Manager
import time
import hashlib
import random as rm


def distance_spatial(s_lon, s_lat, e_lon, e_lat):
    s_rad_lat = s_lat * math.pi / 180.0
    e_rad_lat = e_lat * math.pi / 180.0
    a = s_rad_lat - e_rad_lat
    b = (s_lon - e_lon) * math.pi / 180.0
    s = 2 * math.asin(math.sqrt(math.pow(math.sin(a / 2), 2) + math.cos(s_rad_lat) * math.cos(e_rad_lat)
                                * math.pow(math.sin(b / 2), 2)))
    s = s * 6378137
    s = math.floor(s * 10000) / 10000.0
    return s


def request_core(url, parameters, parse_fun, params_list):
    try:
        response = requests.get(url, params=parameters, timeout=10)
        if response.status_code == 200:
            data = response.text
            return parse_fun(data)
        else:
            params_list.append(parameters)
            print("500")
            return False
    except Exception as e:
        params_list.append(parameters)
        return False


# 返回记录数量
def call_back_fun1(data):
    data_object = json.loads(data)
    poi_number = data_object['count']
    print(poi_number)
    return int(poi_number)


# 将矩形区域按指定分辨率划分为格网便于爬取
def boundary_to_sample_points(boundary, interval_distance):
    point_list = list()

    # 计算横纵向的距离
    distance_x = dt.distance_spatial(boundary[0], 0.5 * (boundary[2] + boundary[3]), boundary[1], 0.5 * (boundary[2] + boundary[3]))
    distance_y = dt.distance_spatial(0.5 * (boundary[0] + boundary[1]), boundary[2], 0.5 * (boundary[0] + boundary[1]), boundary[3])

    # # 计算横纵向的网格个数，
    # x_count = math.floor(distance_x / (interval_distance * 2.0 / math.sqrt(2)))
    # y_count = math.floor(distance_y / (interval_distance * 2.0 / math.sqrt(2)))

    # 计算横纵向的网格个数，
    x_count = math.floor(distance_x / interval_distance) + 1
    y_count = math.floor(distance_y / interval_distance) + 1

    d_x = (boundary[1] - boundary[0]) / x_count
    d_y = (boundary[3] - boundary[2]) / y_count

    # 构造中心点
    for i in range(x_count + 1):
        for j in range(y_count + 1):
            point_list.append([boundary[0] + i * d_x, boundary[2] + j * d_y])

    return point_list


# 返回记录内容
def call_back_fun(data):
    data_list = list()
    try:
        # 解析为Json字符串
        data_object = json.loads(data)
        poi_number = data_object['count']
        poi_list = data_object['pois']
        for one_poi in poi_list:
            poi_name = ""
            if "name" in one_poi.keys():
                poi_name = one_poi['name']
            poi_type = ""
            if "type" in one_poi.keys():
                poi_type = one_poi['type']
            address = ""
            if "address" in one_poi.keys():
                address = one_poi['address']
            longitude = 0
            latitude = 0
            if "location" in one_poi.keys():
                longitude, latitude = one_poi['location'].split(',')
            province_name = ""
            if "pname" in one_poi.keys():
                province_name = one_poi['pname']
            city_name = ""
            if "cityname" in one_poi.keys():
                city_name = one_poi['cityname']
            address_name = ""
            if "adname" in one_poi.keys():
                address_name = one_poi['adname']
            type_code = ""
            if "typecode" in one_poi.keys():
                type_code = one_poi['typecode']
            data_list.append([province_name, city_name, address_name, poi_name, poi_type, type_code, longitude, latitude, address])
        print(len(data_list))
    except Exception as e:
        print(data)
        return False
    return data_list


def clawer_core_1(process_id, url, params_list, running_process, key_list):
    running_process.value += 1
    if len(params_list) == 0:
        running_process.value -= 1
        return
    params = params_list.pop(0)
    poi_number = request_core(url, params, call_back_fun1, params_list)
    if int(poi_number) == 0:
        running_process.value -= 1
        return
    origin_radius = float(params['radius'])
    if int(poi_number) > 700 and origin_radius > 10:
        now_center_location = params['location']
        longitude, latitude = now_center_location.split(',')
        longitude = float(longitude)
        latitude = float(latitude)
        radius = int(float(params['radius']) / 2) + 1

        longitude_degree = (radius / (111319.55 * math.cos(latitude / 180.0 * math.pi)) / math.sqrt(2))
        latitude_degree = (radius / 111319.55 / math.sqrt(2))

        location_left_top = str(round(longitude - longitude_degree, 6)) + ',' + str(round(latitude + latitude_degree, 6))
        location_right_top = str(round(longitude + longitude_degree, 6)) + ',' + str(round(latitude + latitude_degree, 6))
        location_left_bottom = str(round(longitude - longitude_degree, 6)) + ',' + str(round(latitude - latitude_degree, 6))
        location_right_bottom = str(round(longitude + longitude_degree, 6)) + ',' + str(round(latitude - latitude_degree, 6))

        new_params_1 = dict()
        new_params_1['location'] = location_left_bottom
        radius_1 = int(distance_spatial(longitude, latitude, round(longitude - longitude_degree, 6), round(latitude + latitude_degree, 6))) + 1
        radius = max(radius * 2 - radius_1, radius_1)
        new_params_1['radius'] = radius
        new_params_1['key'] = key_list[rm.randint(0, len(key_list) - 1)]
        new_params_1['offset'] = params['offset']
        new_params_1['city'] = params['city']
        new_params_1['types'] = params['types']
        params_list.append(new_params_1)

        new_params_2 = dict()
        new_params_2['location'] = location_right_bottom
        radius_2 = int(distance_spatial(longitude, latitude, round(longitude + longitude_degree, 6), round(latitude + latitude_degree, 6))) + 1
        radius = max(radius * 2 - radius_2, radius_2)
        new_params_2['radius'] = radius
        new_params_2['key'] = key_list[rm.randint(0, len(key_list) - 1)]
        new_params_2['offset'] = params['offset']
        new_params_2['city'] = params['city']
        new_params_2['types'] = params['types']
        params_list.append(new_params_2)

        new_params_3 = dict()
        new_params_3['location'] = location_left_top
        radius_3 = int(distance_spatial(longitude, latitude, round(longitude - longitude_degree, 6),
                                      round(latitude - latitude_degree, 6))) + 1
        radius = max(radius * 2 - radius_3, radius_3)
        new_params_3['radius'] = radius
        new_params_3['key'] = key_list[rm.randint(0, len(key_list) - 1)]
        new_params_3['offset'] = params['offset']
        new_params_3['city'] = params['city']
        new_params_3['types'] = params['types']
        params_list.append(new_params_3)

        new_params_4 = dict()
        new_params_4['location'] = location_right_top
        radius_4 = int(distance_spatial(longitude, latitude, round(longitude + longitude_degree, 6),
                                      round(latitude - latitude_degree, 6))) + 1
        radius = max(radius * 2 - radius_4, radius_4)
        new_params_4['radius'] = radius
        new_params_4['key'] = key_list[rm.randint(0, len(key_list) - 1)]
        new_params_4['offset'] = params['offset']
        new_params_4['city'] = params['city']
        new_params_4['types'] = params['types']
        params_list.append(new_params_4)
    else:
        request_count = math.floor(int(poi_number) / 50 + 1)
        all_poi = list()

        for page in range(request_count):
            params['page'] = page + 1
            data_list = request_core(url, params, call_back_fun, params_list)
            if data_list == False:
                print('error')
                continue
            for record in data_list:
                all_poi.append(record)

        filename = 'magic' + time.strftime('%Y%m%d%H%M%S', time.localtime()) + str(rm.randint(0, 999999))
        md5 = hashlib.md5(filename.encode(encoding='UTF-8'))
        en_filename = md5.hexdigest()
        with open('xiamen/{0}.csv'.format(en_filename), 'w', encoding="utf-8") as f:
            for record in all_poi:
                output_str = ''
                for item in record:
                    output_str += str(item)
                    output_str += ','
                output_str = output_str[:-1] + '\n'
                f.write(output_str)
            f.close()
        all_poi.clear()
    running_process.value -= 1


if __name__ == '__main__':
    process_count = 100

    key_list = ["xxxxx", "xxxxx", "xxxxx"]
    boundary = [118.213, 32.432, 119.321, 33.231]
    city = 'shanghai'
    radius = '10000'

    url = 'https://restapi.amap.com/v3/place/around'
    point_list = boundary_to_sample_points(boundary, 500)

    all_poi = list()
    count = 0

    p_manager = Manager()
    running_process = p_manager.Value('number', 0)

    params_list = p_manager.list()

    for one_point in point_list:
        location = str(round(one_point[0], 6)) + ',' + str(round(one_point[1], 6))
        gd_server_key = key_list[count % len(key_list)]
        parameters = {
            'key': gd_server_key,
            'location': location,
            'city': "\'{0}\'".format(city),
            'radius': radius,
            'offset': 50,
            'types': '010000|020000|030000|040000|050000|060000|070000|080000|090000|100000|110000|120000|130000|140000|150000|160000|170000|180000|190000|200000|210000|220000|990000'
        }
        params_list.append(parameters)
        count += 1

    count = 0
    while len(params_list) != 0 or running_process.value != 0:
        while True:
            if running_process.value < 100:
                p = Process(target=clawer_core_1, args=(str(count), url, params_list, running_process, key_list))
                p.start()
                time.sleep(0.1)
                break
        count += 1
        print('now {0}, {1} processes running'.format(count, running_process.value))

    # 等待所有进程结束
    time.sleep(60)