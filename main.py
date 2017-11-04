import sqlite3
import os
from threading import Thread, Lock

tLock = Lock()


def select_and_print(cursor, query):
    print cursor.execute(query).fetchall()


def get_con_cur(db_name):
    con = sqlite3.connect(db_name)
    cur = con.cursor()
    return con, cur


def obtain_db_for_withdrawal_func_decorator(w_func):

    def wrapper(amount_to_w, w_count):
        con, cur = get_con_cur('/Users/nrichard/python_projects/sqlite_transaction/bank.db')
        w_func(amount_to_w, w_count, con, cur)

    wrapper.__name__ = w_func.__name__ + '_d'

    return wrapper


@obtain_db_for_withdrawal_func_decorator
def execute_withdrawal(amount_withdrawn, w_count, con=None, cur=None):
    # This is the most naive implementation:
    # Balance is accessed first, stored, and re-used.
    # Of course while this happens some other threads may have changed the value of balance already
    balance = cur.execute("SELECT Balance FROM Accounts WHERE Name = 'Gabriel'").fetchone()[0]
    cur.execute("UPDATE Accounts SET Balance = %d WHERE Name = 'Gabriel'" % (balance - amount_withdrawn))
    con.commit()
    w_count[0] += 1


@obtain_db_for_withdrawal_func_decorator
def execute_withdrawal_with_python_lock(amount_withdrawn, w_count, con=None, cur=None):
    # Improvement on the naive 'execute_withdrawal'
    # By requiring a lock to run the 2 step process in 'execute_withdrawal'
    # The threads are forced to run in sequence and they can no longer interact together
    tLock.acquire()
    execute_withdrawal(amount_withdrawn, w_count)
    tLock.release()

@obtain_db_for_withdrawal_func_decorator
def execute_withdrawal_transaction(amount_withdrawn, w_count, con=None, cur=None):
    # Safe approach but does not check for sufficient funds. Allows all withdrawals
    cur.execute(
        "UPDATE Accounts SET Balance = "
            "(SELECT Balance FROM Accounts WHERE Name = 'Gabriel') - %d "
        "WHERE Name = 'Gabriel'" % (amount_withdrawn))
    con.commit()
    w_count[0] += 1


@obtain_db_for_withdrawal_func_decorator
def execute_withdrawal_transaction_if_sufficient_funds(amount_to_w, w_count, con=None, cur=None):
    # Inspired by 'optimistic locking'
    query = """
        update accounts
        set balance = balance - %d
        where Name = 'Gabriel'
        and balance - %d > 0 ;
        """ % (amount_to_w, amount_to_w)
    cur.execute(query)
    con.commit()
    # number of updated rows
    # if 1 row was updated, the withdrawal was successful
    # if 0 row updated, insufficient funds, the withdrawal has been denied
    w_count[0] += cur.rowcount


def setup_db(db_name):
    try:
        os.remove(db)
    except OSError as e:
        print e

    con = sqlite3.connect(db)
    cur = con.cursor()
    cur.execute("CREATE TABLE Accounts(Id INT, Name TEXT, Balance INT)")

    accounts = (
        (1, 'John', 52642),
        (2, 'Kelly', 57127),
        (3, 'Gabriel', 9000),
        (4, 'Jordy', 29000),
        (5, 'Julian', 350000),
    )

    cur.executemany("INSERT INTO Accounts VALUES(?, ?, ?)", accounts)
    con.commit()

    # select_and_print(cur, "SELECT * FROM Accounts")


def make_N_withdrawals(N, amount_per_w, w_function):
    # This functions starts N threads all trying to withdraw from the bank account simultaneously
    threads = []
    withdrawal_count = [0]
    for i in range(N):
        t = Thread(target=w_function, args=(amount_per_w, withdrawal_count), name='t%d' % i)
        threads.append(t)
        t.start()

    # wait for all threads to return
    for t in threads:
        t.join()

    return withdrawal_count[0]


def test_withdrawal_func(withdrawal_function):
    setup_db(db_name)
    con, cur = get_con_cur(db_name)

    initial_balance = cur.execute("SELECT Balance FROM Accounts WHERE Name = 'Gabriel'").fetchone()[0]
    # amount withdrawn per request
    amount_per_w = 5000

    withdrawal_count = make_N_withdrawals(5, amount_per_w, withdrawal_function)

    final_balance = cur.execute("SELECT Balance FROM Accounts WHERE Name = 'Gabriel'").fetchone()[0]
    balance_should_be = initial_balance - withdrawal_count * amount_per_w

    if final_balance == balance_should_be:
        return True
    else:
        return False

        # print "Final balance is: %d" % final_balance
        # print "And should be: %d" % balance_should_be
        # print "We serviced %d withdrawal of %d " % (withdrawal_count, amount_per_w)
        # print "And started at %d" % initial_balance


app_home = '/Users/nrichard/python_projects/sqlite_transaction'
db = 'bank.db'
db_name = os.path.join(app_home, db)


if __name__ == '__main__':

    for w_function in [execute_withdrawal,
                       execute_withdrawal_transaction,
                       execute_withdrawal_transaction_if_sufficient_funds,
                       execute_withdrawal_with_python_lock]:

        it_worked_counter = 0
        number_of_tests = 50
        for i in range(number_of_tests):
            if test_withdrawal_func(w_function):
                it_worked_counter += 1

        print "%s worked %d / %d " % (w_function.__name__, it_worked_counter, number_of_tests)
