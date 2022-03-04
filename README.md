# ParaNames: A multilingual resource for parallel names

This repository contains releases for the ParaNames corpus, as introduced in this [preprint](https://arxiv.org/abs/2202.14035).

See the Releases page for the downloadable archives.

## Notes on corpus

#### Release format 

The corpus is released as a TSV file which is produced by the pipeline included in this repository.

#### Type ambiguity 

Type ambiguity arises when an entity can potentially be associated with multiple types. 
For example, the entity *Brandeis University* can be both a LOC and an ORG, depending on interpretation.

Since the pipeline does not perform any type disambiguation by default, it is possible that an entity ID appears more than once for a given language.
We leave disambiguation up to the end user.

## Notes on code

Will be released with the code release.
