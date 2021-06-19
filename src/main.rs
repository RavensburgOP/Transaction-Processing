use std::{collections::HashMap, env, fs, io};

use processing::{Account, TransactionRow};

use crate::serializing::{read_csv, write_csv};

mod processing;
mod serializing;

fn process_transactions<I>(transactions: I) -> Vec<Account>
where
    I: Iterator<Item = TransactionRow>,
{
    let accounts: HashMap<u16, Account> = HashMap::new();
    let res = transactions
        .fold(accounts, |mut acc, t| {
            let account = acc
                .entry(t.client)
                .or_insert_with(|| Account::new(t.client));
            account.process_transaction(&t);
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
    let input = read_csv(file_content.as_bytes()).filter_map(|r| r.ok());
    let accounts = process_transactions(input);
    let mut writer = io::stdout();
    let output = accounts.iter().map(|a| a.into());
    write_csv(output, &mut writer).unwrap();
}

#[cfg(test)]
mod tests {

    use crate::processing::OutputAccount;

    use super::*;

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

        let deserialized = read_csv(input).filter_map(|r| r.ok());

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

        write_csv(res.into_iter(), &mut writer)
            .map_err(|e| format!("Failed to write csv: {}", e))?;

        let output = std::str::from_utf8(&writer)
            .map_err(|e| format!("Failed converting output from bytes to string: {}", e))?;

        assert_eq!(
            output,
            "client,available,held,total,locked\n1,1.5000,0.0,1.5000,false\n2,2.0000,0.0,2.0000,false\n"
        );

        Ok(())
    }
}
