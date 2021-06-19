use std::io;

use csv::{DeserializeRecordsIntoIter, Trim};
use serde::{de, Deserialize, Deserializer, Serializer};

use crate::processing::{OutputAccount, TransactionRow};

pub(crate) fn read_csv(input: &[u8]) -> DeserializeRecordsIntoIter<&[u8], TransactionRow> {
    csv::ReaderBuilder::new()
        .trim(Trim::All)
        .delimiter(b',')
        .from_reader(input)
        .into_deserialize::<TransactionRow>()
}

// Passing the writer explicitly makes it easier to test the output
pub(crate) fn write_csv<W, I>(output: I, writer: &mut W) -> io::Result<()>
where
    W: io::Write,
    I: Iterator<Item = OutputAccount>,
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

/// Deserialize amount values to u64
pub(crate) fn deserialize_amount<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    // TODO: refactor
    let deserialized = String::deserialize(deserializer)?;
    let split_amount: Vec<&str> = deserialized.split('.').collect();
    let left = split_amount.first().ok_or_else(|| {
        de::Error::invalid_value(
            de::Unexpected::Seq,
            &"Could not find decimal point in amount string",
        )
    })?;

    let right = split_amount.last().ok_or_else(|| {
        de::Error::invalid_value(
            de::Unexpected::Seq,
            &"Could not find decimal point in amount string",
        )
    })?;

    // The test description says, that there will be at most 4 places past the decimal, so standardize all input to that
    let output = format!("{}{:0<4}", left, right);

    output
        .parse::<u64>()
        .map_err(|_| de::Error::invalid_value(de::Unexpected::Str(&output), &"10000"))
}

pub(crate) fn serialize_amount<S>(x: &u64, s: S) -> Result<S::Ok, S::Error>
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

#[cfg(test)]
mod tests {

    use crate::{processing::TransactionType, TransactionRow};

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

        let deserialized: Vec<TransactionRow> = read_csv(input)
            .filter_map(|r| -> Option<TransactionRow> { r.ok() })
            .collect();

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
}
