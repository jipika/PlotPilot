"""
Service for Worldbuilding
"""
from typing import Optional
import uuid

from domain.worldbuilding.worldbuilding import Worldbuilding
from infrastructure.persistence.database.worldbuilding_repository import WorldbuildingRepository
from application.world.worldbuilding_storage import apply_slices_to_entity


class WorldbuildingService:
    """世界观构建服务"""

    def __init__(self, repository: WorldbuildingRepository):
        self.repository = repository

    def get_worldbuilding(self, novel_id: str) -> Optional[Worldbuilding]:
        """获取小说的世界观"""
        return self.repository.get_by_novel_id(novel_id)

    def create_worldbuilding(self, novel_id: str) -> Worldbuilding:
        """创建空白世界观"""
        worldbuilding = Worldbuilding(
            id=f"wb-{uuid.uuid4().hex[:12]}",
            novel_id=novel_id,
        )
        self.repository.save(worldbuilding)
        return worldbuilding

    def update_worldbuilding(
        self,
        novel_id: str,
        core_rules: dict = None,
        geography: dict = None,
        society: dict = None,
        culture: dict = None,
        daily_life: dict = None,
    ) -> Worldbuilding:
        """更新世界观"""
        worldbuilding = self.repository.get_by_novel_id(novel_id)

        if not worldbuilding:
            worldbuilding = self.create_worldbuilding(novel_id)

        slices: dict = {}
        if core_rules:
            slices["core_rules"] = core_rules
        if geography:
            slices["geography"] = geography
        if society:
            slices["society"] = society
        if culture:
            slices["culture"] = culture
        if daily_life:
            slices["daily_life"] = daily_life
        if slices:
            from application.world.worldbuilding_storage import entity_to_canonical_slices

            merged = entity_to_canonical_slices(worldbuilding)
            for dim, blk in slices.items():
                merged.setdefault(dim, {}).update(blk)
            apply_slices_to_entity(worldbuilding, merged)

        self.repository.save(worldbuilding)
        return worldbuilding
