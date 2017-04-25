all: .git/hooks/pre-commit .git/hooks/pre-push

.git/hooks/pre-commit: scripts/pre-commit
	cp scripts/pre-commit .git/hooks/pre-commit 
	chmod +x .git/hooks/pre-commit 
	echo Successfully installed pre-commit hook.

.git/hooks/pre-push: scripts/pre-push
	cp scripts/pre-push .git/hooks/pre-push 
	chmod +x .git/hooks/pre-push
	echo Successfully installed pre-push hook.