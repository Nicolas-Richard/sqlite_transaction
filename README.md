# Some transaction and concurrency tests with Python and Sqlite

This is the classical example where a bank account is spammed by many withdrawal attempts at once.

Several withdrawal functions are implemented. The first one purposedly fails to update the balance on the account correctly. The other functions show ways to work around the problem either by making the operation a Sqlite transaction or using Python's locking mechanism.
