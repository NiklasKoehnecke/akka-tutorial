import pandas
from pandas import DataFrame
import hashlib
import random
import string
from typing import List, Dict


def encrypt_string(hash_string: str) -> str:
    sha_signature: str = hashlib.sha256(hash_string.encode()).hexdigest()
    return sha_signature

candidates = "ABCDEFGHIJKLM"
password_length = 14
characters_in_password = 4
number_of_passwords = 5000
dataset_df = DataFrame()

dataset_raw: List[Dict] = []
for i in range(0,number_of_passwords):
    # Create Password
    random_characters = random.sample(candidates, characters_in_password)
    random_password: str =  ''.join(random.choices(random_characters, k=password_length))

    # Create Hints
    hints: List[str] = []
    for candidate in candidates:
        if candidate in random_characters:
            continue
        hints.append(candidates.replace(candidate, ""))

    # Create Entry 
    line = {
        "ID" : f"{i+1}",
        "Name": f"Name{i+1}",
        "PasswordChars" : candidates,
        "PasswordLength" : f"{password_length}",
        "Password" : encrypt_string(random_password)
    }
    for i in range(0, len(hints)):
        shuffled: str= ''.join(random.sample(hints[i],len(hints[i])))
        line[f"Hint{i+1}"] = encrypt_string(shuffled)

    # Append
    dataset_raw.append(line)

as_df: DataFrame = DataFrame(dataset_raw)
as_df.to_csv("passwords.csv", index=False)