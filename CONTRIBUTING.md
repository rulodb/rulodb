# Contributing to RuloDB

Thank you for your interest in contributing to RuloDB! Your help is appreciated.

## How to Contribute

1. **Fork the repository** and create your branch from `main`.
2. **Open an issue** to discuss your idea or bug before starting work.
3. **Write clear, concise commit messages**.
4. **Submit a pull request** with a description of your changes.

## Coding Style

- Follow [Rust's official style guide](https://doc.rust-lang.org/1.0.0/style/) for Rust code.
- Use `cargo fmt` to format Rust code.
- For TypeScript, use [Prettier](https://prettier.io/) and [ESLint](https://eslint.org/).
- Write clear, maintainable code and add comments where necessary.

## Running Tests

- For RuloDB:
  ```shell
  cargo test
  ```
- For TypeScript SDK:
  ```shell
  npm test
  ```

## Pull Request Checklist

- [ ] Code compiles and passes all tests
- [ ] Linting passes (cargo fmt, cargo clippy, npm run lint, etc.)
- [ ] Documentation is updated if needed
- [ ] Commit messages are signed off (use `git commit -s`), clear and follow the project's conventions
- [ ] Linked to an issue if applicable

## Writing Commit Messages

Please [write a great commit message](https://chris.beams.io/posts/git-commit/).

1. Separate subject from body with a blank line
2. Limit the subject line to 50 characters
3. Do not capitalize the subject line
4. Do not end the subject line with a period
5. Use the imperative mood in the subject line (example: "Fix networking issue")
6. Wrap the body at about 72 characters
7. Use the body to explain **why**, _not what and how_ (the code shows that!)

We use [conventional commits](https://www.conventionalcommits.org/en/v1.0.0/) format. An example excellent commit could
look like this:

```
fix: short summary of changes in 50 chars or less in total

Add a more detailed explanation here, if necessary. Possibly give
some background about the issue being fixed, etc. The body of the
commit message can be several paragraphs. Further paragraphs come
after blank lines and please do proper word-wrap.

Wrap it to about 72 characters or so. In some contexts,
the first line is treated as the subject of the commit and the
rest of the text as the body. The blank line separating the summary
from the body is critical (unless you omit the body entirely);
various tools like `log`, `shortlog` and `rebase` can get confused
if you run the two together.

Explain the problem that this commit is solving. Focus on why you
are making this change as opposed to how or what. The code explains
how or what. Reviewers and your future self can read the patch,
but might not understand why a particular solution was implemented.
Are there side effects or other unintuitive consequences of this
change? Here's the place to explain them.

 - Bullet points are okay, too
 - A hyphen or asterisk should be used for the bullet, preceded
   by a single space, with blank lines in between

Note the fixed or relevant GitHub issues at the end:

Resolves: #123
See also: #456, #789
```

## Code Review

- **Self-review your code before submitting it.** This helps to reduce review cycles. Also, you may find something that
  you would change after all.
- **Review the code, not the author.** Look for and suggest improvements without disparaging or insulting the author.
  Provide **actionable feedback** and explain your reasoning.
- **You are not your code.** When your code is critiqued, questioned, or constructively criticized, remember that you
  are not your code. Do not take code review personally.
- **Always do your best.** No one writes bugs on purpose. Do your best, and learn from your mistakes.
- Kindly note any violations to the guidelines specified in this document.

## Code of Conduct

Please be respectful and follow the [Code of Conduct](./CODE_OF_CONDUCT.md).

## Developer's Certificate of Origin

Developer's Certificate of Origin 1.1

By making a contribution to this project, I certify that:

(a) The contribution was created in whole or in part by me and I have the right to submit it under the open source
license indicated in the file; or

(b) The contribution is based upon previous work that, to the best of my knowledge, is covered under an appropriate open
source license and I have the right under that license to submit that work with modifications, whether created in whole
or in part by me, under the same open source license (unless I am permitted to submit under a different license), as
indicated in the file; or

(c) The contribution was provided directly to me by some other person who certified (a), (b) or (c) and I have not
modified it.

(d) I understand and agree that this project and the contribution are public and that a record of the contribution (
including all personal information I submit with it, including my sign-off) is maintained indefinitely and may be
redistributed consistent with this project or the open source license(s) involved.

<hr />
Thank you for helping make RuloDB better!
