import rpyc
from server import UserData

leynier = None


def test1():
    nodes = rpyc.discover('TRACKER')

    assert len(nodes) == 2

    node_c = nodes[0] if nodes[0][0].split('.')[3] == '13' else nodes[1]
    node_l = nodes[1] if nodes[0][0].split('.')[3] == '13' else nodes[0]

    c_c = rpyc.connect(*node_c)
    c_l = rpyc.connect(*node_l)

    print(c_c.root.client_store(leynier.get_id(), leynier.to_json()))
    c_c.root.client_data()
    c_c.root.client_table()
    c_l.root.client_data()
    c_l.root.client_table()


def test2():
    conn = rpyc.connect('localhost', 8081)
    _, name_time = leynier.get_name()
    leynier.set_name('carlos', name_time + 1)
    print(conn.root.client_store(leynier.get_id(), 123123123123, 1))
    conn.root.client_data()


def test3():
    conn = rpyc.connect('192.168.43.13', 8081)
    print(conn.root.client_find_value(leynier.get_id()))
    conn.root.client_data()


def test4():
    conn = rpyc.connect('localhost', 8081)
    _, name_time = leynier.get_name()
    leynier.set_name('carlos', name_time + 1)
    print(conn.root.client_store(leynier.get_id(), 123123123123, 2))
    conn.root.client_data()


if __name__ == "__main__":
    print('Running test ...')
    leynier = UserData('leynier', '+5353478301', '1234')
    tests = [test1, test2, test3, test4]
    for test in tests:
        input(f'Press key for run the test: {test.__name__} ')
        print(f'Result: ', end='')
        test()
