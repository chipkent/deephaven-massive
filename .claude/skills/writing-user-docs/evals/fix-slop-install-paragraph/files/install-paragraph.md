## Install

The library runs on macOS and Linux, and on Windows through WSL2, on a computer with at least
8 GB of memory. It is not on PyPI, and while its repository is private you need access to it
(see the [tester guide's access step](docs/tester-guide.md#12-get-access-to-the-code)). Outside
testers get access and a version from the owner, and follow the
[tester guide](docs/tester-guide.md), which walks through every step. With access to the
repository, you need Python 3.11 or later, Java 17 or
later (check with `java -version`; any current JDK works, such as one from
[Adoptium](https://adoptium.net)), and a Massive account:

```bash
python3 -m venv ~/dh-massive && source ~/dh-massive/bin/activate
pip install deephaven-server "deephaven-massive @ git+https://github.com/chipkent/deephaven-massive.git"
export MASSIVE_API_KEY="your-api-key"    # from the keys page of Massive's dashboard
deephaven server --port 10000 --jvm-args "-Xmx4g -Dauthentication.psk=choose-a-password"
```

Open <http://localhost:10000/?psk=choose-a-password> and use the **Console** panel; every example
here runs there. (Without `-Dauthentication.psk`, the server makes up a password and prints a
link that carries it in its startup log.) Set the key in the shell before you start the server,
never in the console: every user of the server can read the console's variables.
