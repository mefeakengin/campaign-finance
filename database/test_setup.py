import lda.test_setup
import bills.test_setup
from database.lda.models import *
from database.bills.models import *
import general

def main():
    bills.test_setup.main()
    lda.test_setup.main()

    general.init_db()

    print Bill.query.count() == 2
    b1 = Bill.query.get('111_HR1')
    b2 = Bill.query.get('111_HR2')

    print LobbyingSpecificIssue.query.count() == 1
    i1 = LobbyingSpecificIssue.query.first()
    print i1.bills_by_number == [b1]
    print i1.bills_by_title == [b2]
    print i1.bills_by_title

    general.close_db(write=False)

if __name__ == '__main__':
    main()
