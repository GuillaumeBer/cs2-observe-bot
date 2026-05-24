from collections import deque

class FIFOUniqueCache:
    """
    Structure de données combinant un ensemble (set) et une file (deque)
    pour offrir un dédoublonnement avec complexité O(1) et une éviction
    ordonnée (First-In, First-Out) lorsque la taille maximale est atteinte.
    """
    def __init__(self, maxsize: int):
        if maxsize <= 0:
            raise ValueError("maxsize doit être strictement supérieur à 0")
        self.maxsize = maxsize
        self.queue = deque()
        self.set = set()

    def add(self, item_id: str) -> bool:
        """
        Tente d'ajouter un identifiant au cache.
        Retourne True si l'identifiant est nouveau et a été ajouté,
        False s'il était déjà présent.
        """
        if item_id in self.set:
            return False

        # Éviction du plus ancien si le cache est plein
        if len(self.queue) >= self.maxsize:
            oldest = self.queue.popleft()
            self.set.discard(oldest)

        self.queue.append(item_id)
        self.set.add(item_id)
        return True

    def __contains__(self, item_id: str) -> bool:
        return item_id in self.set

    def __len__(self) -> int:
        return len(self.set)
