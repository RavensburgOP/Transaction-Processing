use std::{collections::HashMap, env, fs, io};

use csv::Trim;
use serde::{
    de::{self, Unexpected},
    Deserialize, Deserializer, Serialize, Serializer,
};

/// Deserialize amount values to u64
fn deserialize_amount<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    // TODO: refactor
    let deserialized = String::deserialize(deserializer)?;
    let split_amount: Vec<&str> = deserialized.split('.').collect();
    let left = split_amount.first().ok_or_else(|| {
        de::Error::invalid_value(
            Unexpected::Seq,
            &"Could not find decimal point in amount string",
        )
    })?;

    let right = split_amount.last().ok_or_else(|| {
        de::Error::invalid_value(
            Unexpected::Seq,
            &"Could not find decimal point in amount string",
        )
    })?;

    // The test description says, that there will be at most 4 places past the decimal, so standardize all input to that
    let output = format!("{}{:0<4}", left, right);

    output
        .parse::<u64>()
        .map_err(|_| de::Error::invalid_value(Unexpected::Str(&output), &"10000"))
}

fn serialize_amount<S>(x: &u64, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if *x == 0 {
        s.serialize_str("0.0")
    } else {
        let amount_string = x.to_string();
        let len = amount_string.len() - 4;
        let first_digits = &amount_string[..len];
        let last_digits = &amount_string[len..];
        s.serialize_str(&format!("{}.{}", first_digits, last_digits))
    }
}

// Floats are  client: (), tx: (), amount: () imprecise, so don't use them for systems handling money
#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
enum TransactionType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
struct TransactionRow {
    pub r#type: TransactionType,
    pub client: u16,
    pub tx: u32,
    #[serde(deserialize_with = "deserialize_amount")]
    pub amount: u64,
}

#[derive(Clone, Debug, PartialEq, PartialOrd)]
enum Resolution {
    Resolve,
    Chargeback,
}

#[derive(Clone, Debug, PartialEq, PartialOrd)]
enum Dispute {
    None,
    Ongoing,
    Done(Resolution),
}

#[derive(Clone, Debug, PartialEq, PartialOrd)]
struct Transaction {
    amount: u64,
    dispute: Dispute,
}

#[derive(Clone, Debug)]
struct Account {
    client: u16,
    available: u64,
    held: u64,
    locked: bool,
    transactions: HashMap<u32, Transaction>,
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "lowercase")]
struct OutputAccount {
    client: u16,
    #[serde(serialize_with = "serialize_amount")]
    available: u64,
    #[serde(serialize_with = "serialize_amount")]
    held: u64,
    #[serde(serialize_with = "serialize_amount")]
    total: u64,
    locked: bool,
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
    fn new(client: u16) -> Self {
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

fn read_csv(input: &[u8]) -> Vec<TransactionRow> {
    csv::ReaderBuilder::new()
        .trim(Trim::All)
        .delimiter(b',')
        .from_reader(input)
        .into_deserialize::<TransactionRow>()
        .filter_map(|r| -> Option<TransactionRow> { r.ok() })
        .collect()
}

fn write_csv<W>(output: Vec<OutputAccount>, writer: &mut W) -> io::Result<()>
where
    W: io::Write,
{
    let mut wrt = csv::WriterBuilder::new()
        .delimiter(b',')
        .has_headers(true)
        .from_writer(writer);

    for row in output {
        wrt.serialize(row)?
    }
    wrt.flush()?;
    Ok(())
}

fn process_transactions(transactions: Vec<TransactionRow>) -> Vec<Account> {
    let accounts: HashMap<u16, Account> = HashMap::new();
    let res = transactions
        .iter()
        .fold(accounts, |mut acc, t| {
            let account = acc
                .entry(t.client)
                .or_insert_with(|| Account::new(t.client));
            account.process_transaction(t);
            acc
        })
        .values()
        .cloned()
        .collect();
    res
}

fn main() {
    let file_path = match env::args_os().nth(1) {
        None => panic!("expected 1 argument, but got none"),
        Some(file_path) => file_path,
    };
    let file_content = match fs::read_to_string(file_path) {
        Ok(content) => content,
        Err(e) => panic!("failed to open file {}", e),
    };
    let input = read_csv(file_content.as_bytes());
    let accounts = process_transactions(input);
    let mut writer = io::stdout();
    let output = accounts.iter().map(|a| a.into()).collect();
    write_csv(output, &mut writer).unwrap();
}

#[cfg(test)]
mod tests {

    use super::*;

    // TODO: test edge cases
    #[test]
    fn can_deserialize_csv() -> Result<(), String> {
        let input = r#"type, client, tx, amount
deposit, 1, 1, 1.0
deposit, 2, 2, 2.0
deposit, 1, 3, 2.0
withdrawal, 1, 4, 1.5
withdrawal, 2, 5, 3.05
"#
        .as_bytes();

        let deserialized = read_csv(input);

        let first_res = deserialized
            .first()
            .ok_or("No first entry in deserialized vector")?;

        assert_eq!(
            first_res,
            &TransactionRow {
                r#type: TransactionType::Deposit,
                client: 1,
                tx: 1,
                amount: 10000
            }
        );

        let last_res = deserialized
            .last()
            .ok_or("No first entry in deserialized vector")?;

        assert_eq!(
            last_res,
            &TransactionRow {
                r#type: TransactionType::Withdrawal,
                client: 2,
                tx: 5,
                amount: 30500
            }
        );

        Ok(())
    }

    #[test]
    fn sanity_check() -> Result<(), String> {
        let input = r#"type, client, tx, amount
deposit, 1, 1, 1.0
deposit, 2, 2, 2.0
deposit, 1, 3, 2.0
withdrawal, 1, 4, 1.5
withdrawal, 2, 5, 3.0
"#
        .as_bytes();

        let deserialized = read_csv(input);

        let mut res: Vec<OutputAccount> = process_transactions(deserialized)
            .iter()
            .map(|a| a.into())
            .collect();

        res.sort_by(|a, b| a.client.partial_cmp(&b.client).unwrap());

        let a = OutputAccount {
            client: 1,
            available: 15000,
            held: 00000,
            total: 15000,
            locked: false,
        };
        let b = OutputAccount {
            client: 2,
            available: 20000,
            held: 00000,
            total: 20000,
            locked: false,
        };
        let exp = vec![a, b];

        assert_eq!(res, exp);

        let mut writer = Vec::new();

        write_csv(res, &mut writer).map_err(|e| format!("Failed to write csv: {}", e))?;

        let output = std::str::from_utf8(&writer)
            .map_err(|e| format!("Failed converting output from bytes to string: {}", e))?;

        assert_eq!(
            output,
            "client,available,held,total,locked\n1,1.5000,0.0,1.5000,false\n2,2.0000,0.0,2.0000,false\n"
        );

        Ok(())
    }

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
