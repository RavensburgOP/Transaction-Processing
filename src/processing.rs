use std::collections::HashMap;

use crate::serializing::{deserialize_amount, serialize_amount};
use serde::{Deserialize, Serialize};

// Floats are imprecise, so don't use them for systems handling money
#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum TransactionType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub(crate) struct TransactionRow {
    pub r#type: TransactionType,
    pub client: u16,
    pub tx: u32,
    #[serde(deserialize_with = "deserialize_amount")]
    pub amount: u64,
}

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub(crate) enum Resolution {
    Resolve,
    Chargeback,
}

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub(crate) enum Dispute {
    None,
    Ongoing,
    Done(Resolution),
}

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub(crate) struct Transaction {
    pub amount: u64,
    pub dispute: Dispute,
}

#[derive(Clone, Debug)]
pub(crate) struct Account {
    pub client: u16,
    pub available: u64,
    pub held: u64,
    pub locked: bool,
    pub transactions: HashMap<u32, Transaction>,
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) struct OutputAccount {
    pub client: u16,
    #[serde(serialize_with = "serialize_amount")]
    pub available: u64,
    #[serde(serialize_with = "serialize_amount")]
    pub held: u64,
    #[serde(serialize_with = "serialize_amount")]
    pub total: u64,
    pub locked: bool,
}

impl From<&Account> for OutputAccount {
    fn from(account: &Account) -> Self {
        OutputAccount {
            client: account.client,
            available: account.available,
            held: account.held,
            total: account.available + account.held,
            locked: account.locked,
        }
    }
}

impl Account {
    pub fn new(client: u16) -> Self {
        Account {
            client,
            available: 0,
            held: 0,
            locked: false,
            transactions: HashMap::new(),
        }
    }

    fn process_deposit(&mut self, transaction: &TransactionRow) {
        self.available += transaction.amount;
    }

    fn process_withdrawal(&mut self, transaction: &TransactionRow) {
        if self.available > transaction.amount {
            self.available -= transaction.amount;
            self.transactions.insert(
                transaction.tx,
                Transaction {
                    amount: transaction.amount,
                    dispute: Dispute::None,
                },
            );
        }
    }

    fn process_dispute(&mut self, transaction: &TransactionRow) {
        if let Some(e) = self.transactions.get_mut(&transaction.tx) {
            if e.dispute == Dispute::None {
                e.dispute = Dispute::Ongoing;
            }
            self.held += e.amount;
        };
    }

    fn process_resolve(&mut self, transaction: &TransactionRow) {
        if let Some(e) = self.transactions.get_mut(&transaction.tx) {
            if e.dispute == Dispute::Ongoing {
                e.dispute = Dispute::Done(Resolution::Resolve);
                self.held -= e.amount;
                self.available += e.amount;
            }
        };
    }

    fn process_chargeback(&mut self, transaction: &TransactionRow) {
        if let Some(e) = self.transactions.get_mut(&transaction.tx) {
            if e.dispute == Dispute::Ongoing {
                e.dispute = Dispute::Done(Resolution::Chargeback);
                self.held -= e.amount;
                self.locked = true;
            }
        };
    }

    pub fn process_transaction(&mut self, transaction: &TransactionRow) {
        // Only non-locked accounts will process new transactions (based on assumption)
        if !self.locked {
            match transaction.r#type {
                TransactionType::Deposit => self.process_deposit(transaction),
                TransactionType::Withdrawal => self.process_withdrawal(transaction),
                TransactionType::Dispute => self.process_dispute(transaction),
                TransactionType::Resolve => self.process_resolve(transaction),
                TransactionType::Chargeback => self.process_chargeback(transaction),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handle_dispute_event_resolve() -> Result<(), String> {
        let client_id = 0;
        let account = Account::new(client_id);

        let transactions = vec![
            TransactionRow {
                r#type: TransactionType::Deposit,
                client: client_id,
                tx: 0,
                amount: 20000,
            },
            TransactionRow {
                r#type: TransactionType::Withdrawal,
                client: client_id,
                tx: 1,
                amount: 10000,
            },
            TransactionRow {
                r#type: TransactionType::Dispute,
                client: client_id,
                tx: 1,
                amount: 0,
            },
        ];

        let mut acc = transactions.iter().fold(account, |mut acc, t| {
            acc.process_transaction(t);
            acc
        });

        assert_eq!(acc.available, 10000);
        assert_eq!(acc.held, 10000);

        acc.process_transaction(&TransactionRow {
            r#type: TransactionType::Resolve,
            client: client_id,
            tx: 1,
            amount: 0,
        });

        assert_eq!(acc.available, 20000);
        assert_eq!(acc.held, 0);
        assert_eq!(acc.locked, false);

        Ok(())
    }

    #[test]
    fn handle_dispute_event_chargeback() -> Result<(), String> {
        let client_id = 0;
        let account = Account::new(client_id);

        let transactions = vec![
            TransactionRow {
                r#type: TransactionType::Deposit,
                client: client_id,
                tx: 0,
                amount: 20000,
            },
            TransactionRow {
                r#type: TransactionType::Withdrawal,
                client: client_id,
                tx: 1,
                amount: 10000,
            },
            TransactionRow {
                r#type: TransactionType::Dispute,
                client: client_id,
                tx: 1,
                amount: 0,
            },
        ];

        let mut acc = transactions.iter().fold(account, |mut acc, t| {
            acc.process_transaction(t);
            acc
        });

        assert_eq!(acc.available, 10000);
        assert_eq!(acc.held, 10000);

        acc.process_transaction(&TransactionRow {
            r#type: TransactionType::Chargeback,
            client: client_id,
            tx: 1,
            amount: 0,
        });

        assert_eq!(acc.available, 10000);
        assert_eq!(acc.held, 0);
        assert_eq!(acc.locked, true);

        Ok(())
    }

    #[test]
    fn no_transactions_for_locked_account() -> Result<(), String> {
        let client_id = 0;
        let mut account = Account::new(client_id);
        account.locked = true;

        let transactions = vec![
            TransactionRow {
                r#type: TransactionType::Deposit,
                client: client_id,
                tx: 0,
                amount: 20000,
            },
            TransactionRow {
                r#type: TransactionType::Withdrawal,
                client: client_id,
                tx: 1,
                amount: 10000,
            },
            TransactionRow {
                r#type: TransactionType::Dispute,
                client: client_id,
                tx: 1,
                amount: 0,
            },
        ];

        let acc = transactions.iter().fold(account, |mut acc, t| {
            acc.process_transaction(t);
            acc
        });

        assert_eq!(acc.available, 0);
        assert_eq!(acc.held, 0);

        Ok(())
    }
}
